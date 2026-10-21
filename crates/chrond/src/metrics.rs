//! Prometheus metrics: counters and histograms updated as things happen,
//! and a collector that reads every database's state on each scrape.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{
    Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

use crate::node::{Node, Role};

/// Seconds, from a millisecond to half a minute: the gRPC call buckets.
pub const BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Seconds, from 100 µs to half a minute: commits and sync waits, which
/// take well under a millisecond without fsync.
pub const COMMIT_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 30.0,
];

/// Every `chron.v1.Chron` method and whether it streams.
pub const METHODS: &[(&str, bool)] = &[
    ("/chron.v1.Chron/Commit", false),
    ("/chron.v1.Chron/ReadAggregate", true),
    ("/chron.v1.Chron/ReadAll", true),
    ("/chron.v1.Chron/ReadTag", true),
    ("/chron.v1.Chron/AggregateLen", false),
    ("/chron.v1.Chron/Head", false),
    ("/chron.v1.Chron/Subscribe", true),
    ("/chron.v1.Chron/Ack", false),
    ("/chron.v1.Chron/ListCursors", false),
    ("/chron.v1.Chron/DeleteCursor", false),
    ("/chron.v1.Chron/Flush", false),
    ("/chron.v1.Chron/Info", false),
    ("/chron.v1.Chron/Backup", false),
    ("/chron.v1.Chron/ApplyBackup", false),
    ("/chron.v1.Chron/Retain", false),
    ("/chron.v1.Chron/Replicate", true),
    ("/chron.v1.Chron/Promote", false),
    ("/chron.v1.Chron/Fence", false),
    ("/chron.v1.Chron/GetMembers", false),
    ("/chron.v1.Chron/ChangeMembers", false),
    ("/chron.v1.Chron/CreateDatabase", false),
    ("/chron.v1.Chron/ListDatabases", false),
    ("/chron.v1.Chron/DropDatabase", false),
    ("/chron.v1.Chron/Heartbeat", false),
    ("/chron.v1.Chron/RequestVote", false),
    ("/chron.v1.Chron/Discover", false),
    ("/chron.v1.Chron/Fingerprints", false),
    ("/chron.v1.Chron/Follow", true),
    ("/chron.v1.Chron/FetchBackup", true),
    ("/grpc.health.v1.Health/Check", false),
    ("/grpc.health.v1.Health/Watch", true),
];

/// The method label for a request path: the path itself when it is a
/// known method, else `unknown` (so arbitrary paths cannot grow the
/// label set). The bool says whether it streams.
pub fn method_label(path: &str) -> (&'static str, bool) {
    METHODS
        .iter()
        .find(|(m, _)| *m == path)
        .map(|(m, s)| (*m, *s))
        .unwrap_or(("unknown", false))
}

/// gRPC status codes spelled as go-grpc-prometheus does, so existing
/// dashboards apply.
pub fn code_label(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "Canceled",
        2 => "Unknown",
        3 => "InvalidArgument",
        4 => "DeadlineExceeded",
        5 => "NotFound",
        6 => "AlreadyExists",
        7 => "PermissionDenied",
        8 => "ResourceExhausted",
        9 => "FailedPrecondition",
        10 => "Aborted",
        11 => "OutOfRange",
        12 => "Unimplemented",
        13 => "Internal",
        14 => "Unavailable",
        15 => "DataLoss",
        16 => "Unauthenticated",
        _ => "Unknown",
    }
}

/// How a commit went, for `chron_commits_total`.
pub fn commit_result(r: &Result<chron::Committed, tonic::Status>) -> &'static str {
    match r {
        Ok(c) if c.duplicate => "duplicate",
        Ok(_) => "ok",
        Err(s) => match s.code() {
            tonic::Code::FailedPrecondition
                if s.metadata()
                    .contains_key(chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER) =>
            {
                "conflict"
            }
            tonic::Code::FailedPrecondition => "refused",
            tonic::Code::DeadlineExceeded
                if s.metadata()
                    .contains_key(chron_proto::COMMITTED_NEXT_HEADER) =>
            {
                "unconfirmed"
            }
            tonic::Code::InvalidArgument => "invalid",
            tonic::Code::ResourceExhausted => "too_large",
            _ => "error",
        },
    }
}

pub struct Metrics {
    registry: Registry,
    pub rpc_calls: IntCounterVec,
    pub rpc_duration: HistogramVec,
    pub streams_open: IntGaugeVec,
    pub commits: IntCounterVec,
    pub commit_duration: HistogramVec,
    pub replication_errors: IntCounterVec,
    pub chunks_applied: IntCounterVec,
    pub http_requests: IntCounterVec,
    /// Campaigns this node started.
    pub elections: IntCounter,
    /// Times this node's role changed because of the cluster: elected,
    /// fenced by a peer, stepped down.
    pub leader_changes: IntCounter,
    /// Heartbeats a peer did not answer, by peer.
    pub heartbeat_failures: IntCounterVec,
    /// Time from the local commit to a majority's confirmation.
    pub sync_wait: HistogramVec,
    /// Sync commits answered "unconfirmed", by reason.
    pub sync_timeouts: IntCounterVec,
    /// FetchBackup streams served, by result.
    pub backup_fetches: IntCounterVec,
    /// Databases this daemon replaced with a copy fetched from the leader.
    pub restores: IntCounterVec,
    /// Membership generations this node committed as the leader.
    pub member_changes: IntCounter,
    /// Durable cursors per database.
    pub cursors: IntGaugeVec,
    /// Per cursor: the confirmed bound minus its next, at its last
    /// acknowledgment or registration.
    pub cursor_lag: IntGaugeVec,
    /// Acknowledgments by outcome.
    pub cursor_acks: IntCounterVec,
    start_time: IntGauge,
}

fn counter(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric")
}

fn gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric")
}

fn histogram(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    histogram_with(name, help, labels, BUCKETS)
}

fn histogram_with(name: &str, help: &str, labels: &[&str], buckets: &[f64]) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid metric")
}

impl Metrics {
    pub fn new() -> Arc<Metrics> {
        let registry = Registry::new();
        let rpc_calls = counter(
            "chron_rpc_calls_total",
            "gRPC calls completed, by method and status code",
            &["db", "method", "code"],
        );
        let rpc_duration = histogram(
            "chron_rpc_duration_seconds",
            "gRPC call duration from request to end of stream",
            &["db", "method"],
        );
        let streams_open = gauge(
            "chron_streams_open",
            "server streams currently open",
            &["db", "method"],
        );
        let commits = counter(
            "chron_commits_total",
            "Commit calls by result: ok, conflict, refused, unconfirmed, invalid, too_large, error",
            &["db", "result"],
        );
        let commit_duration = histogram_with(
            "chron_commit_duration_seconds",
            "Commit duration including the write gate, the fsync and, for sync commits, the wait for a majority",
            &["db"],
            COMMIT_BUCKETS,
        );
        let sync_wait = histogram_with(
            "chron_sync_commit_wait_seconds",
            "time from the local commit to a majority's confirmation, for sync commits that were confirmed",
            &["db"],
            COMMIT_BUCKETS,
        );
        let sync_timeouts = counter(
            "chron_sync_commit_timeouts_total",
            "sync commits answered unconfirmed: timeout, or role_change",
            &["db", "reason"],
        );
        let replication_errors = counter(
            "chron_replication_errors_total",
            "replication failures recorded by a replica",
            &["db"],
        );
        let chunks_applied = counter(
            "chron_replication_chunks_applied_total",
            "replication chunks applied by a replica",
            &["db"],
        );
        let http_requests = counter(
            "chron_http_requests_total",
            "HTTP gateway requests by route and status",
            &["route", "status"],
        );
        let elections = IntCounter::new(
            "chron_elections_total",
            "campaigns this node started (pre-vote included)",
        )
        .expect("valid metric");
        let cursors = gauge(
            "chron_cursors",
            "durable cursors kept on the database",
            &["db"],
        );
        let cursor_lag = gauge(
            "chron_cursor_lag_events",
            "confirmed bound minus the cursor's next, at its last acknowledgment",
            &["db", "cursor"],
        );
        let cursor_acks = counter(
            "chron_cursor_acks_total",
            "cursor acknowledgments by outcome (ok, noop, taken_over, ahead, not_found, refused)",
            &["db", "result"],
        );
        let member_changes = IntCounter::new(
            "chron_member_changes_total",
            "membership generations this node committed as the leader (its own republish included)",
        )
        .expect("valid metric");
        let leader_changes = IntCounter::new(
            "chron_leader_changes_total",
            "role changes driven by the cluster: elected, fenced by a peer, stepped down",
        )
        .expect("valid metric");
        let heartbeat_failures = counter(
            "chron_heartbeat_failures_total",
            "heartbeats a peer did not answer",
            &["peer"],
        );
        let backup_fetches = counter(
            "chron_backup_fetches_total",
            "FetchBackup streams by result: ok, error, cancelled",
            &["db", "result"],
        );
        let restores = counter(
            "chron_restores_total",
            "databases replaced by a copy fetched from the leader, by result: ok, error",
            &["db", "result"],
        );
        let build_info = gauge("chron_build_info", "chrond version", &["version"]);
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        let start_time = IntGauge::new(
            "chron_start_time_seconds",
            "when the daemon started, unix seconds",
        )
        .expect("valid metric");
        start_time.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        );
        for c in [
            Box::new(rpc_calls.clone()) as Box<dyn Collector>,
            Box::new(rpc_duration.clone()),
            Box::new(streams_open.clone()),
            Box::new(commits.clone()),
            Box::new(commit_duration.clone()),
            Box::new(replication_errors.clone()),
            Box::new(chunks_applied.clone()),
            Box::new(http_requests.clone()),
            Box::new(elections.clone()),
            Box::new(member_changes.clone()),
            Box::new(cursors.clone()),
            Box::new(cursor_lag.clone()),
            Box::new(cursor_acks.clone()),
            Box::new(leader_changes.clone()),
            Box::new(heartbeat_failures.clone()),
            Box::new(sync_wait.clone()),
            Box::new(sync_timeouts.clone()),
            Box::new(backup_fetches.clone()),
            Box::new(restores.clone()),
            Box::new(build_info),
            Box::new(start_time.clone()),
        ] {
            registry.register(c).expect("unique metric");
        }
        Arc::new(Metrics {
            registry,
            rpc_calls,
            rpc_duration,
            streams_open,
            commits,
            commit_duration,
            replication_errors,
            chunks_applied,
            http_requests,
            elections,
            member_changes,
            cursors,
            cursor_lag,
            cursor_acks,
            leader_changes,
            heartbeat_failures,
            sync_wait,
            sync_timeouts,
            backup_fetches,
            restores,
            start_time,
        })
    }

    /// Registers the collector that reads the node's databases on scrape.
    pub fn attach(&self, node: &Arc<Node>) {
        self.registry
            .register(Box::new(NodeCollector::new(Arc::downgrade(node))))
            .expect("one node per registry");
    }

    /// The Prometheus text exposition. Reads every database's info, so
    /// run it on the blocking pool.
    pub fn render(&self) -> String {
        let families = self.registry.gather();
        TextEncoder::new()
            .encode_to_string(&families)
            .unwrap_or_default()
    }

    pub fn started_at(&self) -> i64 {
        self.start_time.get()
    }

    /// Observes one commit.
    pub fn commit(&self, db: &str, start: Instant, r: &Result<chron::Committed, tonic::Status>) {
        self.commits
            .with_label_values(&[db, commit_result(r)])
            .inc();
        self.commit_duration
            .with_label_values(&[db])
            .observe(start.elapsed().as_secs_f64());
    }
}

/// On every scrape: head, floor, segments, bytes, epoch, fsync, role,
/// replication state and the library's commit counters, per database.
struct NodeCollector {
    node: Weak<Node>,
    head: IntGaugeVec,
    floor: IntGaugeVec,
    segments: IntGaugeVec,
    segment_bytes: IntGaugeVec,
    epoch: IntGaugeVec,
    fsync_enabled: IntGaugeVec,
    role: IntGaugeVec,
    fenced_by: IntGaugeVec,
    replica_connected: IntGaugeVec,
    leader_head: IntGaugeVec,
    lag: IntGaugeVec,
    lease_remaining: Gauge,
    leader_known: IntGauge,
    voted_epoch: IntGauge,
    peers: IntGauge,
    members: IntGauge,
    members_version: IntGauge,
    members_epoch: IntGauge,
    members_committed: IntGauge,
    member: IntGauge,
    sync_commit: IntGauge,
    majority_applied: IntGaugeVec,
    follower_applied: IntGaugeVec,
    follower_counted: IntGaugeVec,
    events: IntCounterVec,
    transactions: IntCounterVec,
    batches: IntCounterVec,
    fsyncs: IntCounterVec,
    fsync_seconds: prometheus::CounterVec,
    index_checkpoints: IntCounterVec,
    chunk_ring_hits: IntCounterVec,
    chunk_ring_misses: IntCounterVec,
    /// `chron_fsync_seconds`: built by hand from the library's bucket
    /// counts on each scrape.
    fsync_histogram: Desc,
    last_stats: Mutex<HashMap<String, chron::Stats>>,
}

impl NodeCollector {
    fn new(node: Weak<Node>) -> Self {
        NodeCollector {
            node,
            head: gauge(
                "chron_head",
                "one past the last committed position",
                &["db"],
            ),
            floor: gauge("chron_floor", "the first readable position", &["db"]),
            segments: gauge("chron_segments", "segment files", &["db"]),
            segment_bytes: gauge("chron_segment_bytes", "bytes in segment files", &["db"]),
            epoch: gauge("chron_epoch", "the fencing epoch", &["db"]),
            fsync_enabled: gauge(
                "chron_fsync_enabled",
                "1 when commits sync to disk",
                &["db"],
            ),
            role: gauge(
                "chron_role",
                "1 for the daemon's current role, 0 for the others",
                &["db", "role"],
            ),
            fenced_by: gauge(
                "chron_fenced_by_epoch",
                "the epoch that fenced this daemon (present while fenced)",
                &["db"],
            ),
            replica_connected: gauge(
                "chron_replica_connected",
                "1 while a replica holds a stream from its leader (replicas only)",
                &["db"],
            ),
            leader_head: gauge(
                "chron_leader_head",
                "the leader's head as the replica last saw it",
                &["db"],
            ),
            lag: gauge(
                "chron_replication_lag_events",
                "events the replica is behind its leader",
                &["db"],
            ),
            lease_remaining: Gauge::new(
                "chron_lease_remaining_seconds",
                "seconds left of the leader's majority lease (leaders with peers)",
            )
            .expect("valid metric"),
            leader_known: IntGauge::new(
                "chron_leader_known",
                "1 when this node knows a live leader (itself included)",
            )
            .expect("valid metric"),
            voted_epoch: IntGauge::new("chron_voted_epoch", "the epoch of the last vote given")
                .expect("valid metric"),
            peers: IntGauge::new("chron_peers", "the other members of the membership")
                .expect("valid metric"),
            members: IntGauge::new("chron_members", "members in the membership this node holds")
                .expect("valid metric"),
            members_version: IntGauge::new(
                "chron_members_version",
                "the membership generation's version",
            )
            .expect("valid metric"),
            members_epoch: IntGauge::new(
                "chron_members_epoch",
                "the epoch the membership generation was published under",
            )
            .expect("valid metric"),
            members_committed: IntGauge::new(
                "chron_members_committed",
                "1 when the membership generation is committed (as this node knows; a leader tracks a pending change)",
            )
            .expect("valid metric"),
            member: IntGauge::new("chron_member", "1 when this node is in the membership")
                .expect("valid metric"),
            sync_commit: IntGauge::new(
                "chron_sync_commit",
                "1 when commits wait for a majority by default",
            )
            .expect("valid metric"),
            majority_applied: gauge(
                "chron_majority_applied",
                "the highest position a majority durably holds (leaders with peers)",
                &["db"],
            ),
            follower_applied: gauge(
                "chron_follower_applied_head",
                "the position a follower last reported applied (leaders with peers)",
                &["db", "peer"],
            ),
            follower_counted: gauge(
                "chron_follower_counted",
                "1 when the follower's acknowledgments count toward the majority",
                &["db", "peer"],
            ),
            events: counter(
                "chron_events_committed_total",
                "events landed, by the daemon or by replication",
                &["db"],
            ),
            transactions: counter(
                "chron_transactions_committed_total",
                "transactions landed",
                &["db"],
            ),
            batches: counter(
                "chron_commit_batches_total",
                "index commits that landed transactions (group commit batches)",
                &["db"],
            ),
            fsyncs: counter(
                "chron_fsyncs_total",
                "segment syncs on the commit path",
                &["db"],
            ),
            fsync_seconds: prometheus::CounterVec::new(
                Opts::new("chron_fsync_seconds_total", "time spent in those syncs"),
                &["db"],
            )
            .expect("valid metric"),
            index_checkpoints: counter(
                "chron_index_checkpoints_total",
                "durable index commits: the commit path's checkpoints and every maintenance write; each is one fsync of index.redb (the index's own syncs are not observable otherwise)",
                &["db"],
            ),
            chunk_ring_hits: counter(
                "chron_chunk_ring_hits_total",
                "replication chunks cut from the in-memory ring of recent batches",
                &["db"],
            ),
            chunk_ring_misses: counter(
                "chron_chunk_ring_misses_total",
                "replication chunks that read the segment files",
                &["db"],
            ),
            fsync_histogram: Desc::new(
                "chron_fsync_seconds".to_string(),
                "segment sync duration on the commit path".to_string(),
                vec!["db".to_string()],
                HashMap::new(),
            )
            .expect("valid metric"),
            last_stats: Mutex::new(HashMap::new()),
        }
    }

    /// `chron_fsync_seconds{db}` for every database, from the library's
    /// cumulative counters.
    fn fsync_families(&self, node: &Node) -> MetricFamily {
        use prometheus::proto::{Bucket, Histogram, LabelPair, Metric, MetricType};
        let mut family = MetricFamily::default();
        family.set_name(self.fsync_histogram.fq_name.clone());
        family.set_help(self.fsync_histogram.help.clone());
        family.set_field_type(MetricType::HISTOGRAM);
        let mut metrics = Vec::new();
        for dbn in node.databases() {
            let st = dbn.db.stats();
            let mut h = Histogram::default();
            h.set_sample_count(st.fsyncs);
            h.set_sample_sum(st.fsync_nanos as f64 / 1e9);
            let mut cumulative = 0u64;
            let mut buckets = Vec::new();
            for (i, bound) in chron::FSYNC_BUCKET_BOUNDS.iter().enumerate() {
                cumulative += st.fsync_buckets[i];
                let mut b = Bucket::default();
                b.set_cumulative_count(cumulative);
                b.set_upper_bound(*bound);
                buckets.push(b);
            }
            h.set_bucket(buckets);
            let mut label = LabelPair::default();
            label.set_name("db".to_string());
            label.set_value(dbn.name.clone());
            let mut m = Metric::default();
            m.set_label(vec![label]);
            m.set_histogram(h);
            metrics.push(m);
        }
        family.set_metric(metrics);
        family
    }

    fn collectors(&self) -> Vec<&dyn Collector> {
        vec![
            &self.head,
            &self.floor,
            &self.segments,
            &self.segment_bytes,
            &self.epoch,
            &self.fsync_enabled,
            &self.role,
            &self.fenced_by,
            &self.replica_connected,
            &self.leader_head,
            &self.lag,
            &self.lease_remaining,
            &self.leader_known,
            &self.voted_epoch,
            &self.peers,
            &self.members,
            &self.members_version,
            &self.members_epoch,
            &self.members_committed,
            &self.member,
            &self.sync_commit,
            &self.majority_applied,
            &self.follower_applied,
            &self.follower_counted,
            &self.events,
            &self.transactions,
            &self.batches,
            &self.fsyncs,
            &self.fsync_seconds,
            &self.index_checkpoints,
            &self.chunk_ring_hits,
            &self.chunk_ring_misses,
        ]
    }

    fn refresh(&self) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        let role = node.role();
        // Gauges that are "present only while ..." are reset so a database
        // that left that state (or was dropped) stops reporting.
        for g in [
            &self.fenced_by,
            &self.replica_connected,
            &self.leader_head,
            &self.lag,
            &self.majority_applied,
            &self.follower_applied,
            &self.follower_counted,
        ] {
            g.reset();
        }
        self.sync_commit.set(node.wants_sync(None) as i64);
        self.lease_remaining
            .set(node.lease_remaining().unwrap_or_default().as_secs_f64());
        self.leader_known.set(
            (match role {
                Role::Leader => node.lease_held(),
                Role::Replica => true,
                Role::Candidate | Role::Fenced | Role::Removed => node.heard_leader().is_some(),
            }) as i64,
        );
        self.voted_epoch
            .set(node.lease().voted.map_or(0, |(e, _)| e as i64));
        let st = node.members_state();
        if node.cluster.is_some() {
            self.peers
                .set(st.current.addrs.len().saturating_sub(1) as i64);
            self.members.set(st.current.addrs.len() as i64);
            self.members_version.set(st.current.version as i64);
            self.members_epoch.set(st.current.epoch as i64);
            self.members_committed.set(st.committed as i64);
            self.member.set(node.is_member() as i64);
        } else {
            self.peers.set(0);
            self.members.set(0);
            self.members_version.set(0);
            self.members_epoch.set(0);
            self.members_committed.set(0);
            self.member.set(1);
        }
        let mut last = self.last_stats.lock().unwrap_or_else(|p| p.into_inner());
        for dbn in node.databases() {
            let db = dbn.name.as_str();
            self.head.with_label_values(&[db]).set(dbn.db.head() as i64);
            self.floor
                .with_label_values(&[db])
                .set(dbn.db.floor() as i64);
            self.epoch
                .with_label_values(&[db])
                .set(dbn.db.epoch() as i64);
            if let Ok(info) = dbn.db.info() {
                self.segments
                    .with_label_values(&[db])
                    .set(info.segments as i64);
                self.segment_bytes
                    .with_label_values(&[db])
                    .set(info.bytes as i64);
                self.fsync_enabled
                    .with_label_values(&[db])
                    .set((info.fsync == chron::Fsync::Always) as i64);
            }
            for (r, name) in [
                (Role::Leader, "leader"),
                (Role::Replica, "replica"),
                (Role::Fenced, "fenced"),
                (Role::Candidate, "candidate"),
            ] {
                self.role
                    .with_label_values(&[db, name])
                    .set((role == r) as i64);
            }
            if role == Role::Fenced {
                self.fenced_by
                    .with_label_values(&[db])
                    .set(node.fenced_by().unwrap_or(0) as i64);
            }
            if role == Role::Leader && node.cluster.is_some() {
                self.majority_applied
                    .with_label_values(&[db])
                    .set(dbn.sync.majority() as i64);
                for f in dbn.sync.snapshot(&st.current) {
                    self.follower_applied
                        .with_label_values(&[db, &f.peer])
                        .set(f.applied as i64);
                    self.follower_counted
                        .with_label_values(&[db, &f.peer])
                        .set(f.counted as i64);
                }
            }
            if role == Role::Replica {
                let st = dbn.replica_status();
                self.replica_connected
                    .with_label_values(&[db])
                    .set(st.connected as i64);
                if let Some(lh) = st.leader_head {
                    self.leader_head.with_label_values(&[db]).set(lh as i64);
                    self.lag
                        .with_label_values(&[db])
                        .set(lh.saturating_sub(dbn.db.head()) as i64);
                }
            }
            let now = dbn.db.stats();
            let prev = last.insert(db.to_string(), now).unwrap_or_default();
            self.events
                .with_label_values(&[db])
                .inc_by(now.events.saturating_sub(prev.events));
            self.transactions
                .with_label_values(&[db])
                .inc_by(now.transactions.saturating_sub(prev.transactions));
            self.batches
                .with_label_values(&[db])
                .inc_by(now.batches.saturating_sub(prev.batches));
            self.fsyncs
                .with_label_values(&[db])
                .inc_by(now.fsyncs.saturating_sub(prev.fsyncs));
            self.fsync_seconds
                .with_label_values(&[db])
                .inc_by(now.fsync_nanos.saturating_sub(prev.fsync_nanos) as f64 / 1e9);
            self.index_checkpoints
                .with_label_values(&[db])
                .inc_by(now.index_checkpoints.saturating_sub(prev.index_checkpoints));
            self.chunk_ring_hits
                .with_label_values(&[db])
                .inc_by(now.chunk_ring_hits.saturating_sub(prev.chunk_ring_hits));
            self.chunk_ring_misses
                .with_label_values(&[db])
                .inc_by(now.chunk_ring_misses.saturating_sub(prev.chunk_ring_misses));
        }
    }
}

impl Collector for NodeCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut out: Vec<&Desc> = self
            .collectors()
            .into_iter()
            .flat_map(|c| c.desc())
            .collect();
        out.push(&self.fsync_histogram);
        out
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        let mut out: Vec<MetricFamily> = self
            .collectors()
            .into_iter()
            .flat_map(|c| c.collect())
            .collect();
        if let Some(node) = self.node.upgrade() {
            out.push(self.fsync_families(&node));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_methods_share_one_label() {
        assert_eq!(method_label("/chron.v1.Chron/Nope"), ("unknown", false));
        assert_eq!(
            method_label("/chron.v1.Chron/Subscribe"),
            ("/chron.v1.Chron/Subscribe", true)
        );
        assert_eq!(code_label(9), "FailedPrecondition");
        assert_eq!(code_label(99), "Unknown");
    }
}
