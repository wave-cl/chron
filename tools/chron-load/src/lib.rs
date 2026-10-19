//! A load generator for chron daemons that checks invariants while it
//! runs: writers, readers and subscribers against a leader, periodic
//! backups restored and checked offline, periodic retention, and a
//! replica compared at the end.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chron_client::{Client, ClientConfig, ClientError, Event, Expected, NewEvent, Uuid};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

/// Kills the daemon at an address (a SIGKILL to its pid, or a test's
/// shutdown of an in-process node) and says whether it did. What restarts
/// it is the caller's business. The address is the one the daemon
/// advertises (`Discover` names it), which is how `--pid` keys it.
pub type Killer = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// A writer stamps an acknowledgment after the call returns, so one
/// acknowledged just before a leader died can carry a time a little after
/// the kill was stamped. Half a second covers that, and no successor
/// leads within half a second of a kill: an election waits at least
/// 0.77 of a lease ttl (the last heartbeat may predate the kill by a
/// third of the ttl, and the silence timer is 1.1 to 1.5 of it), so
/// the groups this tool is run against must use a ttl of a second or
/// more, as the daemon's default (10 s) and the tests (1 s) do.
const SLACK: Duration = Duration::from_millis(500);

/// What one run does.
#[derive(Clone)]
pub struct Config {
    /// The leader, or a comma list of a failover group's nodes: each is
    /// asked who leads.
    pub addr: String,
    pub replica: Option<String>,
    pub token: Option<String>,
    pub database: Option<String>,
    pub duration: Duration,
    pub writers: usize,
    pub readers: usize,
    pub subscribers: usize,
    pub aggregates: u64,
    pub events_per_txn: usize,
    pub payload_bytes: usize,
    /// The daemon's backup directory on this host, for restoring what
    /// the daemon wrote; `None` skips backups.
    pub backup_dir: Option<std::path::PathBuf>,
    pub backup_every: Option<Duration>,
    pub retain_every: Option<Duration>,
    pub seed: u64,
    /// Kill the leader this often (needs `killer`); the run then checks
    /// that no write acknowledged by a surviving leader is lost, and
    /// that writes lost with a killed leader were acknowledged before
    /// the kill at positions past every other node's head then.
    pub kill_every: Option<Duration>,
    pub killer: Option<Killer>,
    /// Wait for a majority (`Some(true)`), do not (`Some(false)`), or
    /// take the daemon's default (`None`).
    pub sync: Option<bool>,
}

impl Config {
    pub fn addrs(&self) -> Vec<String> {
        self.addr
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string)
            .collect()
    }

    fn kill_mode(&self) -> bool {
        self.kill_every.is_some() && self.killer.is_some()
    }
}

/// Counts and timings of a run.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub transactions: u64,
    pub events: u64,
    pub conflicts: u64,
    pub errors: u64,
    pub reads: u64,
    pub events_read: u64,
    pub subscribed_events: u64,
    pub backups: u64,
    pub retentions: u64,
    pub commit_p50_ms: f64,
    pub commit_p99_ms: f64,
    pub final_head: u64,
    pub replica_head: Option<u64>,
    /// Leaders killed (kill mode).
    pub kills: u64,
    /// Acknowledged writes not in the log at the end (kill mode): each
    /// one allowed only as a write to a killed leader.
    pub lost_acks: u64,
    /// Calls that failed while a leader was being replaced (kill mode):
    /// expected, counted, not violations.
    pub failover_errors: u64,
    /// Sync commits the leader could not get confirmed in time: landed
    /// locally, not acknowledged to the writer, not counted as acks.
    pub unconfirmed: u64,
    /// Every invariant that failed, with where and what.
    pub violations: Vec<String>,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.violations.is_empty() && self.errors == 0
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "transactions": self.transactions, "events": self.events, "conflicts": self.conflicts,
            "errors": self.errors, "reads": self.reads, "events_read": self.events_read,
            "subscribed_events": self.subscribed_events, "backups": self.backups,
            "retentions": self.retentions, "commit_p50_ms": self.commit_p50_ms,
            "commit_p99_ms": self.commit_p99_ms, "final_head": self.final_head,
            "replica_head": self.replica_head, "kills": self.kills, "lost_acks": self.lost_acks,
            "failover_errors": self.failover_errors, "unconfirmed": self.unconfirmed,
            "violations": self.violations, "ok": self.ok(),
        })
    }
}

/// A small deterministic generator (xorshift64*), one per worker.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    pub fn draw(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.draw() % n }
    }
}

struct Shared {
    stop: AtomicBool,
    transactions: AtomicU64,
    events: AtomicU64,
    conflicts: AtomicU64,
    errors: AtomicU64,
    reads: AtomicU64,
    events_read: AtomicU64,
    subscribed: AtomicU64,
    backups: AtomicU64,
    retentions: AtomicU64,
    floor: AtomicU64,
    latencies: Mutex<Vec<u64>>,
    violations: std::sync::Mutex<Vec<String>>,
    kill_mode: bool,
    kills: AtomicU64,
    failover_errors: AtomicU64,
    unconfirmed: AtomicU64,
    /// Kill mode: every acknowledged write as (position, payload hash,
    /// when).
    acks: std::sync::Mutex<Vec<(u64, u64, Instant)>>,
    /// Kill mode: each kill as (when the leader was down, the highest
    /// head among the other nodes just before it was killed).
    kill_log: std::sync::Mutex<Vec<(Instant, u64)>>,
}

impl Shared {
    /// Records a violation; the first hundred are kept in full, every
    /// one is counted.
    fn violation(&self, what: String) {
        let mut v = self.violations.lock().unwrap_or_else(|p| p.into_inner());
        if v.len() < 100 {
            v.push(what);
        }
    }

    /// Whether `e` is what a failover looks like from a client: the
    /// node is not the leader, cannot be reached, or the call timed out.
    /// Anything else (Internal, Unknown, DataLoss, ...) is a real error.
    fn failover_error(e: &ClientError) -> bool {
        match e {
            ClientError::Fenced { .. }
            | ClientError::Unavailable(_)
            | ClientError::Transport(_)
            | ClientError::NoLeader(_) => true,
            ClientError::Status(s) => matches!(
                s.code(),
                tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
            ),
            _ => false,
        }
    }

    /// Records an error: in kill mode a failover-shaped one is counted,
    /// not a violation.
    fn error(&self, what: &str, e: &ClientError) {
        if self.kill_mode && Self::failover_error(e) {
            self.failover_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.violation(format!("{what}: unexpected error: {e}"));
    }

    fn ack(&self, position: u64, payload: &[u8]) {
        if self.kill_mode {
            self.acks.lock().unwrap_or_else(|p| p.into_inner()).push((
                position,
                hash(payload),
                Instant::now(),
            ));
        }
    }

    /// Whether a kill happened at or after `t`, with the same slack as
    /// the acknowledgment check.
    fn killed_since(&self, t: Instant) -> bool {
        self.kill_log
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|(k, _)| *k + SLACK >= t)
    }
}

/// FNV-1a over a payload, for the ack log.
fn hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Reconnects a client to the leader after a failover-shaped error, in
/// kill mode; otherwise does nothing.
async fn after_error(shared: &Shared, client: &Client, e: &ClientError) {
    if shared.kill_mode && Shared::failover_error(e) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = client.rediscover().await;
    }
}

pub fn aggregate(n: u64) -> Uuid {
    Uuid::from_u128(0x1000 + u128::from(n))
}

/// `w<writer>:s<seq>:a<agg>:i<index>` and filler to `payload_bytes`.
fn payload(writer: usize, seq: u64, agg: u64, index: u64, bytes: usize) -> Vec<u8> {
    let mut p = format!("w{writer}:s{seq}:a{agg}:i{index}:").into_bytes();
    while p.len() < bytes {
        p.push(b'x');
    }
    p
}

/// The aggregate number and index the payload claims.
fn claimed(payload: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut agg = None;
    let mut index = None;
    for part in text.split(':') {
        if let Some(a) = part.strip_prefix('a') {
            agg = a.parse().ok();
        } else if let Some(i) = part.strip_prefix('i') {
            index = i.parse().ok();
        }
    }
    Some((agg?, index?))
}

/// Checks one event against what its payload claims and its position.
fn check_event(shared: &Shared, where_: &str, ev: &Event, expect_position: Option<u64>) {
    if let Some(p) = expect_position
        && ev.position != p
    {
        shared.violation(format!(
            "{where_}: position {} where {p} was expected",
            ev.position
        ));
    }
    let Some((agg, index)) = claimed(&ev.payload) else {
        shared.violation(format!("{where_}: payload at {} is not ours", ev.position));
        return;
    };
    if ev.aggregate != Some(aggregate(agg)) || ev.index != index {
        shared.violation(format!(
            "{where_}: event at {} claims aggregate {agg} index {index}, the record says {:?} #{}",
            ev.position, ev.aggregate, ev.index
        ));
    }
}

fn client_config(cfg: &Config) -> ClientConfig {
    ClientConfig {
        token: cfg.token.clone(),
        admin_token: cfg.token.clone(),
        database: cfg.database.clone(),
        connect_timeout: Duration::from_secs(2),
        request_timeout: Some(Duration::from_secs(30)),
        sync_commit: cfg.sync,
        ..ClientConfig::default()
    }
}

/// A client for the leader: any of the addresses is asked who leads.
async fn connect_leader(cfg: &Config) -> Result<Client, ClientError> {
    Client::connect_any(cfg.addrs(), client_config(cfg)).await
}

async fn connect(cfg: &Config, addr: &str) -> Result<Client, ClientError> {
    Client::connect(addr, client_config(cfg)).await
}

async fn writer(cfg: Config, client: Client, shared: Arc<Shared>, w: usize) {
    let mut rng = Rng::new(cfg.seed ^ ((w as u64 + 1) * 0x9E37));
    // Per aggregate: the length last seen and when it was last acknowledged.
    let mut lens: HashMap<u64, (u64, Instant)> = HashMap::new();
    let mut seq = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        let agg = rng.below(cfg.aggregates);
        let id = aggregate(agg);
        let (len, acked_at) = match lens.get(&agg) {
            Some(l) => *l,
            None => match client.aggregate_len(id).await {
                Ok(l) => (l, Instant::now()),
                Err(e) => {
                    shared.error("writer len", &e);
                    after_error(&shared, &client, &e).await;
                    continue;
                }
            },
        };
        let payloads: Vec<Vec<u8>> = (0..cfg.events_per_txn)
            .map(|j| payload(w, seq, agg, len + j as u64, cfg.payload_bytes))
            .collect();
        let events: Vec<NewEvent> = payloads
            .iter()
            .map(|p| NewEvent::new(p.clone()).tag("load"))
            .collect();
        let n = events.len() as u64;
        let start = Instant::now();
        match client.append(id, Expected::Exact(len), events).await {
            Ok(c) => {
                shared
                    .latencies
                    .lock()
                    .await
                    .push(start.elapsed().as_nanos() as u64);
                shared.transactions.fetch_add(1, Ordering::Relaxed);
                shared.events.fetch_add(n, Ordering::Relaxed);
                if c.appends[0].first_index != len || c.appends[0].new_len != len + n {
                    shared.violation(format!(
                        "writer {w}: expected first index {len} and length {}, got {} and {}",
                        len + n,
                        c.appends[0].first_index,
                        c.appends[0].new_len
                    ));
                }
                for (j, p) in payloads.iter().enumerate() {
                    shared.ack(c.first + j as u64, p);
                }
                lens.insert(agg, (len + n, Instant::now()));
                seq += 1;
            }
            Err(ClientError::Unconfirmed { .. }) => {
                // Landed on the leader, unconfirmed: not acknowledged, so
                // not an ack to check; the length is read again.
                shared.unconfirmed.fetch_add(1, Ordering::Relaxed);
                lens.remove(&agg);
            }
            Err(ClientError::Conflict { actual, .. }) => {
                shared.conflicts.fetch_add(1, Ordering::Relaxed);
                // Shrinking is what a killed leader's lost writes look
                // like; anything else is a violation.
                if actual < len && !shared.killed_since(acked_at) {
                    shared.violation(format!(
                        "writer {w}: aggregate {agg} shrank from {len} to {actual}"
                    ));
                }
                lens.insert(agg, (actual, Instant::now()));
            }
            Err(e) => {
                shared.error("writer commit", &e);
                after_error(&shared, &client, &e).await;
            }
        }
    }
}

async fn reader(client: Client, shared: Arc<Shared>, r: usize, seed: u64) {
    let mut rng = Rng::new(seed ^ ((r as u64 + 100) * 0x7F4A));
    while !shared.stop.load(Ordering::Relaxed) {
        let head = match client.head().await {
            Ok(h) => h,
            Err(e) => {
                shared.error("reader head", &e);
                after_error(&shared, &client, &e).await;
                continue;
            }
        };
        let floor = shared.floor.load(Ordering::Relaxed);
        if head <= floor {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let from = floor + rng.below(head - floor);
        let limit = 1 + rng.below(256) as usize;
        let backward = rng.below(4) == 0;
        let result = if backward {
            client.read_all_backward(from, limit).await
        } else {
            client.read_all(from, limit).await
        };
        match result {
            Ok(events) => {
                shared.reads.fetch_add(1, Ordering::Relaxed);
                shared
                    .events_read
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                if !backward && events.is_empty() {
                    shared.violation(format!("reader {r}: nothing at {from} below head {head}"));
                }
                for (i, ev) in events.iter().enumerate() {
                    let expect = if backward {
                        from.checked_sub(i as u64)
                    } else {
                        Some(from + i as u64)
                    };
                    check_event(&shared, &format!("reader {r}"), ev, expect);
                }
            }
            Err(ClientError::BelowFloor { floor: f, .. }) => {
                // Retention moved the floor since we looked; remember it.
                shared.floor.fetch_max(f, Ordering::Relaxed);
            }
            Err(ClientError::PositionOutOfRange { .. }) if shared.kill_mode => {
                // The head we read came from a leader since replaced.
            }
            Err(e) => {
                shared.error("reader", &e);
                after_error(&shared, &client, &e).await;
            }
        }
    }
}

async fn subscriber(client: Client, shared: Arc<Shared>, s: usize) {
    let mut next = shared.floor.load(Ordering::Relaxed);
    'outer: while !shared.stop.load(Ordering::Relaxed) {
        let mut stream = match client.subscribe(next).await {
            Ok(s) => s,
            Err(ClientError::PositionOutOfRange { head, .. }) if shared.kill_mode => {
                // Positions past the new leader's head were lost with
                // the old one; they will be reused.
                next = head;
                continue;
            }
            Err(e) => {
                shared.error("subscribe", &e);
                if shared.kill_mode && Shared::failover_error(&e) {
                    after_error(&shared, &client, &e).await;
                    continue;
                }
                return;
            }
        };
        while !shared.stop.load(Ordering::Relaxed) {
            let item = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
            match item {
                Ok(Some(Ok(ev))) => {
                    check_event(&shared, &format!("subscriber {s}"), &ev, Some(next));
                    next = ev.position + 1;
                    shared.subscribed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Some(Err(e))) => {
                    shared.error("subscriber", &e);
                    if shared.kill_mode && Shared::failover_error(&e) {
                        after_error(&shared, &client, &e).await;
                        continue 'outer;
                    }
                    return;
                }
                Ok(None) => {
                    shared.violation(format!("subscriber {s}: the stream ended"));
                    return;
                }
                Err(_) => {}
            }
        }
    }
}

/// Kill mode: every `kill_every`, notes the highest head among the
/// nodes that are not the leader, kills the leader, and records the
/// moment it is down: anything it acknowledged before that moment at a
/// position past that head may be lost.
async fn killer(cfg: Config, client: Client, shared: Arc<Shared>) {
    let (Some(every), Some(kill)) = (cfg.kill_every, cfg.killer.clone()) else {
        return;
    };
    // `last` moves only on a kill that happened, so a miss (no leader
    // known yet, a node still rejoining) is retried a moment later.
    let mut last = Instant::now();
    while !shared.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if last.elapsed() < every {
            continue;
        }
        let leader = match client.discover().await {
            Ok(d) => match d.leader_addr {
                Some(a) => a,
                None => continue,
            },
            Err(e) => {
                after_error(&shared, &client, &e).await;
                continue;
            }
        };
        // The successor is the most up-to-date node among those that
        // vote; with three nodes every other node that follows the
        // leader votes, so the highest head among them bounds what the
        // successor holds. The kill waits until every other node is a
        // connected follower, so the bound means something and a node
        // still rejoining (or cutting back a tail of its own, and ahead)
        // is never counted.
        let others: Vec<String> = cfg.addrs().into_iter().filter(|a| *a != leader).collect();
        let mut bound = 0;
        let mut connected = 0;
        for addr in &others {
            if let Ok(c) = connect(&cfg, addr).await
                && let Ok(i) = c.info().await
                && i.role == chron_client::Role::Replica
                && i.replica_connected
            {
                bound = bound.max(i.head);
                connected += 1;
            }
        }
        if connected < others.len() {
            continue;
        }
        if !kill(leader.clone()).await {
            eprintln!("the leader {leader} was not killed; trying again shortly");
            continue;
        }
        shared
            .kill_log
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((Instant::now(), bound));
        shared.kills.fetch_add(1, Ordering::Relaxed);
        last = Instant::now();
    }
}

/// Kill mode, at the end: every acknowledged write is in the log, except
/// ones acknowledged before a kill at positions past the other nodes'
/// heads at that kill.
async fn check_acks(shared: &Shared, leader: &Client) -> Result<u64, ClientError> {
    // Taken, not cloned, and walked in position order against the log's
    // pages: memory stays at one copy of the acknowledgments.
    let mut acks = std::mem::take(&mut *shared.acks.lock().unwrap_or_else(|p| p.into_inner()));
    acks.sort_unstable_by_key(|(p, _, _)| *p);
    let kills = shared
        .kill_log
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let floor = leader.info().await?.floor;
    let head = leader.head().await?;
    let mut lost = 0;
    let mut i = acks.partition_point(|(p, _, _)| *p < floor);
    let mut missing: Vec<(u64, u64, Instant)> = Vec::new();
    let mut from = floor;
    while from < head {
        let page = leader.read_all(from, 4096).await?;
        if page.is_empty() {
            break;
        }
        for ev in &page {
            while i < acks.len() && acks[i].0 < ev.position {
                missing.push(acks[i]);
                i += 1;
            }
            let h = hash(&ev.payload);
            while i < acks.len() && acks[i].0 == ev.position {
                if acks[i].1 != h {
                    missing.push(acks[i]);
                }
                i += 1;
            }
        }
        from = page.last().map_or(head, |e| e.position + 1);
    }
    missing.extend_from_slice(&acks[i..]);
    for (position, _, at) in missing {
        lost += 1;
        let allowed = kills
            .iter()
            .any(|(k, bound)| at <= *k + SLACK && position >= *bound);
        if !allowed {
            let timeline: Vec<String> = kills
                .iter()
                .enumerate()
                .map(|(i, (k, bound))| {
                    let rel = if at <= *k {
                        format!("-{:?}", k.duration_since(at))
                    } else {
                        format!("+{:?}", at.duration_since(*k))
                    };
                    format!("kill {i}: acked {rel} relative, bound {bound}")
                })
                .collect();
            shared.violation(format!(
                "acknowledged write at {position} is not in the log and no kill explains it ({})",
                timeline.join("; ")
            ));
        }
    }
    Ok(lost)
}

/// Backups restored and checked offline, and retention, on their timers.
async fn admin(cfg: Config, client: Client, shared: Arc<Shared>) {
    let mut last_backup = Instant::now();
    let mut last_retain = Instant::now();
    let mut n = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let (Some(every), Some(dir)) = (cfg.backup_every, &cfg.backup_dir)
            && last_backup.elapsed() >= every
        {
            last_backup = Instant::now();
            n += 1;
            let name = format!("load-{n}.chronbak");
            match client.backup(&name, None).await {
                Ok(meta) => {
                    shared.backups.fetch_add(1, Ordering::Relaxed);
                    let archive = dir
                        .join(cfg.database.as_deref().unwrap_or("default"))
                        .join(&name);
                    let lens: Vec<(u64, u64)> = {
                        let mut v = Vec::new();
                        for a in 0..cfg.aggregates {
                            match client.aggregate_len(aggregate(a)).await {
                                Ok(l) => v.push((a, l)),
                                Err(e) => shared.error("admin len", &e),
                            }
                        }
                        v
                    };
                    let shared2 = shared.clone();
                    let check = tokio::task::spawn_blocking(move || {
                        check_backup(&shared2, &archive, meta.head, &lens)
                    })
                    .await;
                    if let Err(e) = check {
                        shared.violation(format!("backup check task: {e}"));
                    }
                    let _ = std::fs::remove_file(
                        dir.join(cfg.database.as_deref().unwrap_or("default"))
                            .join(&name),
                    );
                }
                Err(e) => shared.error("backup", &e),
            }
        }
        if let Some(every) = cfg.retain_every
            && last_retain.elapsed() >= every
        {
            last_retain = Instant::now();
            match client.head().await {
                Ok(head) => match client.retain(head / 2).await {
                    Ok(r) => {
                        shared.retentions.fetch_add(1, Ordering::Relaxed);
                        shared.floor.fetch_max(r.floor, Ordering::Relaxed);
                        if r.floor > head / 2 {
                            shared.violation(format!(
                                "retain to {} moved the floor to {}",
                                head / 2,
                                r.floor
                            ));
                        }
                    }
                    Err(e) => shared.error("retain", &e),
                },
                Err(e) => shared.error("admin head", &e),
            }
        }
    }
}

/// Restores a full backup into a temp dir and checks it is a consistent
/// prefix: head as the archive says, dense positions, payloads agreeing
/// with their records, every aggregate's length at most the live one.
fn check_backup(shared: &Shared, archive: &std::path::Path, head: u64, lens: &[(u64, u64)]) {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            shared.violation(format!("backup tempdir: {e}"));
            return;
        }
    };
    let target = dir.path().join("restored");
    if let Err(e) = chron::restore_backup(archive, &target, &chron::Options::new()) {
        shared.violation(format!("restore {}: {e}", archive.display()));
        return;
    }
    let db = match chron::Db::open(
        &target,
        chron::Options::new()
            .fsync(chron::Fsync::Never)
            .verify_all_segments(true),
    ) {
        Ok(db) => db,
        Err(e) => {
            shared.violation(format!("open restored backup: {e}"));
            return;
        }
    };
    if db.head() != head {
        shared.violation(format!(
            "restored head {} differs from the archive's {head}",
            db.head()
        ));
    }
    let floor = db.floor();
    let mut expect = floor;
    for ev in db.iter_all(floor) {
        match ev {
            Ok(ev) => {
                check_event(shared, "restored backup", &ev, Some(expect));
                expect += 1;
            }
            Err(e) => {
                shared.violation(format!("restored backup read: {e}"));
                break;
            }
        }
    }
    if expect != head {
        shared.violation(format!(
            "restored backup holds {} events from {floor}, head {head}",
            expect - floor
        ));
    }
    for (a, live) in lens {
        match db.aggregate_len(aggregate(*a)) {
            Ok(l) if l > *live => shared.violation(format!(
                "restored aggregate {a} has {l} events, the live one {live}"
            )),
            Ok(_) => {}
            Err(e) => shared.violation(format!("restored len {a}: {e}")),
        }
    }
}

/// Runs the configured load for its duration and reports.
pub async fn run(cfg: Config) -> anyhow::Result<Report> {
    let leader = connect_leader(&cfg).await?;
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        transactions: AtomicU64::new(0),
        events: AtomicU64::new(0),
        conflicts: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        reads: AtomicU64::new(0),
        events_read: AtomicU64::new(0),
        subscribed: AtomicU64::new(0),
        backups: AtomicU64::new(0),
        retentions: AtomicU64::new(0),
        floor: AtomicU64::new(leader.info().await?.floor),
        latencies: Mutex::new(Vec::new()),
        violations: std::sync::Mutex::new(Vec::new()),
        kill_mode: cfg.kill_mode(),
        kills: AtomicU64::new(0),
        failover_errors: AtomicU64::new(0),
        unconfirmed: AtomicU64::new(0),
        acks: std::sync::Mutex::new(Vec::new()),
        kill_log: std::sync::Mutex::new(Vec::new()),
    });
    let mut tasks = Vec::new();
    for w in 0..cfg.writers {
        tasks.push(tokio::spawn(writer(
            cfg.clone(),
            leader.clone(),
            shared.clone(),
            w,
        )));
    }
    for r in 0..cfg.readers {
        tasks.push(tokio::spawn(reader(
            leader.clone(),
            shared.clone(),
            r,
            cfg.seed,
        )));
    }
    for s in 0..cfg.subscribers {
        tasks.push(tokio::spawn(subscriber(leader.clone(), shared.clone(), s)));
    }
    tasks.push(tokio::spawn(admin(
        cfg.clone(),
        leader.clone(),
        shared.clone(),
    )));
    if cfg.kill_mode() {
        tasks.push(tokio::spawn(killer(
            cfg.clone(),
            leader.clone(),
            shared.clone(),
        )));
    }
    tokio::time::sleep(cfg.duration).await;
    shared.stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(30), t).await;
    }

    if cfg.kill_mode() {
        // The last kill may still be settling.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match leader.rediscover().await {
                Ok(_) => break,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(200)).await
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    let lost_acks = if cfg.kill_mode() {
        check_acks(&shared, &leader).await?
    } else {
        0
    };
    let final_head = leader.head().await?;
    let mut replica_head = None;
    if let Some(addr) = &cfg.replica {
        let replica = connect(&cfg, addr).await?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let h = replica.head().await?;
            if h >= final_head {
                replica_head = Some(h);
                break;
            }
            if Instant::now() > deadline {
                shared.violation(format!(
                    "replica stuck at {h} while the leader is at {final_head}"
                ));
                replica_head = Some(h);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if replica_head == Some(final_head) {
            for a in 0..cfg.aggregates {
                let (l, r) = (
                    leader.aggregate_len(aggregate(a)).await?,
                    replica.aggregate_len(aggregate(a)).await?,
                );
                if l != r {
                    shared.violation(format!("aggregate {a}: leader {l}, replica {r}"));
                }
            }
            let floor = shared
                .floor
                .load(Ordering::Relaxed)
                .max(replica.info().await?.floor);
            let mut rng = Rng::new(cfg.seed ^ 0xABCD);
            for _ in 0..10 {
                if final_head <= floor {
                    break;
                }
                let from = floor + rng.below(final_head - floor);
                let (a, b) = (
                    leader.read_all(from, 100).await?,
                    replica.read_all(from, 100).await?,
                );
                let pa: Vec<_> = a.iter().map(|e| (e.position, e.payload.clone())).collect();
                let pb: Vec<_> = b.iter().map(|e| (e.position, e.payload.clone())).collect();
                if pa != pb {
                    shared.violation(format!("page at {from} differs between leader and replica"));
                }
            }
        }
    }

    let mut lat = shared.latencies.lock().await.clone();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            0.0
        } else {
            let i = ((lat.len() - 1) as f64 * p) as usize;
            lat[i] as f64 / 1e6
        }
    };
    Ok(Report {
        transactions: shared.transactions.load(Ordering::Relaxed),
        events: shared.events.load(Ordering::Relaxed),
        conflicts: shared.conflicts.load(Ordering::Relaxed),
        errors: shared.errors.load(Ordering::Relaxed),
        reads: shared.reads.load(Ordering::Relaxed),
        events_read: shared.events_read.load(Ordering::Relaxed),
        subscribed_events: shared.subscribed.load(Ordering::Relaxed),
        backups: shared.backups.load(Ordering::Relaxed),
        retentions: shared.retentions.load(Ordering::Relaxed),
        commit_p50_ms: pct(0.5),
        commit_p99_ms: pct(0.99),
        final_head,
        replica_head,
        kills: shared.kills.load(Ordering::Relaxed),
        lost_acks,
        failover_errors: shared.failover_errors.load(Ordering::Relaxed),
        unconfirmed: shared.unconfirmed.load(Ordering::Relaxed),
        violations: shared
            .violations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone(),
    })
}
