//! Automatic failover among peers (`--peer`): a leader keeps a majority
//! lease with heartbeats and refuses writes once it expires; followers
//! that hear no leader for a lease elect one of themselves.
//!
//! Safety rests on time read from each node's own monotonic clock. A
//! leader reads `t0` before a heartbeat round and, on a majority of
//! acknowledgments, holds its lease until `t0 + ttl`. A follower that
//! acknowledged did so after `t0`, and grants a vote only once it has
//! heard no leader for `ttl`, that is at or after `t0 + ttl`. Any two
//! majorities share a node, so no new leader is elected before the old
//! leader's lease has expired; what can still be in flight are the
//! commits that passed the lease check an instant before. An elected
//! leader's votes are its first lease, measured the same way. A follower
//! waits out the longer of its own ttl and the one its leader announces,
//! so a group with mismatched settings errs on the slow side.
//!
//! A campaign asks for pre-votes first (nothing recorded on either side)
//! and sends real vote requests only when a majority would grant: a lost
//! campaign leaves no promise behind on any node, neither the candidate's
//! own vote (recorded only once the round has won, under the transition
//! lock) nor a voter's. A forced campaign (an operator's Promote) is
//! granted by the live leader, which fences itself first, or by nodes
//! that have heard no leader for a lease; it never lets two leaders
//! overlap.
//!
//! Membership (which nodes vote and count) is a generation `(epoch,
//! version)` with the full set of addresses, persisted on every node
//! and carried on every heartbeat and vote request. A node behind the
//! sender's generation adopts it (persisting it first), one ahead
//! refuses and answers with its own. A change (one address at a time,
//! on the leader, from a committed generation) is committed once a
//! majority of the new set acknowledged at it and durably holds every
//! database's head from when it was made; a majority of the old set and
//! one of the new set always share a node, and once the new one is
//! committed no majority of the old can form, so any two live
//! majorities intersect and the lease argument above holds across
//! changes. A new leader republishes the membership under its own epoch
//! and commits that before taking a change, so a dead leader's
//! uncommitted change at a lower epoch can never outrank it. A heartbeat
//! or vote request from an address outside the membership is refused.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chron_client::ClientError;
use chron_proto::v1 as pb;
use tokio::task::JoinHandle;
use tonic::Status;

use crate::node::{ClusterConfig, Members, Node, Role};
use crate::replica;

/// Heartbeats go out this often; a peer call is bounded by the same.
pub fn tick(ttl: Duration) -> Duration {
    (ttl / 3).max(Duration::from_millis(10))
}

fn ms(d: Duration) -> u64 {
    d.as_millis().min(u128::from(u64::MAX)) as u64
}

/// The cluster task: heartbeats while leading, an election timer
/// otherwise. Runs until the daemon stops.
pub fn spawn(node: Arc<Node>) -> JoinHandle<()> {
    tokio::spawn(async move { run(node).await })
}

async fn run(node: Arc<Node>) {
    let Some(cfg) = node.cluster.clone() else {
        return;
    };
    let ttl = cfg.lease_ttl;
    let mut jitter = Jitter::new(&cfg.advertise);
    // When the quiet period began: the last leader heard, or the last
    // thing this node did (a lost campaign, a step-down), whichever is
    // later, so a lost campaign is not followed by another at once.
    let mut quiet_since = Instant::now();
    loop {
        if node.cancel.is_cancelled() || node.is_stopping() {
            return;
        }
        match node.role() {
            Role::Leader => {
                heartbeat_round(&node, &cfg).await;
                if node.role() == Role::Leader && !node.lease_held() {
                    let n = node.clone();
                    let _ = tokio::task::spawn_blocking(move || n.step_down()).await;
                    node.metrics.leader_changes.inc();
                    quiet_since = Instant::now();
                }
                tokio::select! {
                    _ = node.cancel.cancelled() => return,
                    _ = tokio::time::sleep(tick(ttl)) => {}
                }
            }
            // Not a member: the leader does not heartbeat this node, so it
            // looks for the leader itself (through its seeds and the
            // members it knows), follows it for reads, and adopts the
            // membership the leader holds; one that names this node makes
            // it a replica again.
            Role::Removed => {
                removed_round(&node, &cfg).await;
                tokio::select! {
                    _ = node.cancel.cancelled() => return,
                    _ = tokio::time::sleep(tick(ttl)) => {}
                }
            }
            // A fenced node campaigns too, above the epoch that fenced
            // it: the leader that fenced it may have died before it ever
            // heard from it.
            Role::Replica | Role::Candidate | Role::Fenced => {
                let since = node.heard_at().map_or(quiet_since, |h| h.max(quiet_since));
                let silence = node.silence_ttl();
                let deadline = since + silence + jitter.next(silence);
                let wait = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = node.cancel.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
                if node.heard_leader().is_some() || node.role() == Role::Leader {
                    continue;
                }
                match campaign(&node, false).await {
                    Ok((epoch, head)) => {
                        tracing::info!(epoch, head, "elected leader");
                    }
                    Err(s) => {
                        tracing::info!(reason = s.message(), "election lost");
                    }
                }
                quiet_since = Instant::now();
            }
        }
    }
}

/// Election timeouts spread between `ttl/10` and `ttl/2` so two followers
/// rarely campaign at once (xorshift seeded by the node's address and
/// the wall clock at start).
struct Jitter(u64);

impl Jitter {
    fn new(seed: &str) -> Jitter {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in seed.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x100000001b3);
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        h ^= nanos.wrapping_mul(0x9E3779B97F4A7C15);
        Jitter(h.max(1))
    }

    fn next(&mut self, ttl: Duration) -> Duration {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        let lo = ttl / 10;
        let span = ttl / 2 - lo;
        lo + Duration::from_nanos(x % span.as_nanos().max(1) as u64)
    }
}

/// A call to `addr` with the cluster's credentials, bounded by one tick.
/// A simulated partition answers as unreachable.
async fn call<T, F, Fut>(node: &Node, cfg: &ClusterConfig, addr: &str, f: F) -> Result<T, String>
where
    F: FnOnce(chron_client::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, ClientError>>,
{
    if node.partitioned_from(addr) {
        return Err("partitioned (test)".into());
    }
    let up = cfg.upstream(addr);
    let attempt = async {
        let client = replica::connect(&up).await.map_err(|e| e.to_string())?;
        f(client).await.map_err(|e| e.to_string())
    };
    match tokio::time::timeout(tick(cfg.lease_ttl), attempt).await {
        Ok(r) => r,
        Err(_) => Err(format!(
            "{addr} did not answer within {:?}",
            tick(cfg.lease_ttl)
        )),
    }
}

/// One round of heartbeats to every member of the last committed and
/// the current membership. A majority of the current membership (this
/// node included when it is in it) extends the lease from the instant
/// before the round and, when a change is pending and that majority
/// also holds every head from when it was made, commits it; a peer at a
/// newer epoch fences this node.
pub async fn heartbeat_round(node: &Arc<Node>, cfg: &ClusterConfig) {
    let t0 = Instant::now();
    let epoch = node.data_epoch();
    let st = node.members_state();
    let req = pb::HeartbeatRequest {
        epoch,
        leader_addr: cfg.advertise.clone(),
        head: node.default_db().db.head(),
        lease_ttl_ms: ms(cfg.lease_ttl),
        members: Some(st.current.to_wire()),
    };
    let targets = node.fanout();
    let answers = futures_join(targets.iter().map(|peer| {
        let req = req.clone();
        async move {
            (
                peer.clone(),
                call(node, cfg, peer, |c| async move { c.heartbeat(req).await }).await,
            )
        }
    }))
    .await;
    let mut acks = usize::from(st.current.contains(&cfg.advertise));
    let mut newer: Option<u64> = None;
    for (peer, answer) in answers {
        match answer {
            Ok(r) if r.ok => {
                if st.current.contains(&peer) {
                    acks += 1;
                }
            }
            Ok(r) => {
                tracing::debug!(%peer, epoch = r.epoch, "heartbeat not acknowledged");
                if r.epoch > epoch {
                    newer = Some(newer.map_or(r.epoch, |n| n.max(r.epoch)));
                }
            }
            Err(e) => {
                tracing::debug!(%peer, error = %e, "heartbeat failed");
                node.metrics
                    .heartbeat_failures
                    .with_label_values(&[&peer])
                    .inc();
            }
        }
    }
    if let Some(e) = newer {
        tracing::warn!(epoch = e, "a peer is at a newer epoch; fencing");
        let n = node.clone();
        let _ = tokio::task::spawn_blocking(move || n.fence(e)).await;
        node.metrics.leader_changes.inc();
        return;
    }
    if acks >= st.current.quorum() {
        node.extend_lease(t0 + cfg.lease_ttl);
        #[cfg(feature = "test-util")]
        let catch_up_ok = node
            .skip_catchup_rule
            .load(std::sync::atomic::Ordering::Relaxed)
            || node.new_majority_holds_created_heads();
        #[cfg(not(feature = "test-util"))]
        let catch_up_ok = node.new_majority_holds_created_heads();
        if !st.committed && catch_up_ok && node.mark_members_committed(st.current.generation()) {
            tracing::info!(members = %st.current.oneline(), "membership committed");
            node.metrics.member_changes.inc();
            if !st.current.contains(&cfg.advertise) {
                let n = node.clone();
                let _ = tokio::task::spawn_blocking(move || n.step_down_removed()).await;
                node.metrics.leader_changes.inc();
            }
        }
    }
}

/// One round of a removed node: asks its seeds and the members it knows
/// who leads, adopts a higher membership the leader holds, and follows
/// the leader when it does not already.
async fn removed_round(node: &Arc<Node>, cfg: &ClusterConfig) {
    let mut candidates: Vec<String> = cfg.seeds.clone();
    for a in node.members().others(&cfg.advertise) {
        if !candidates.contains(&a) {
            candidates.push(a);
        }
    }
    let mut leader: Option<String> = None;
    for addr in &candidates {
        let d = call(node, cfg, addr, |c| async move { c.discover().await }).await;
        if let Ok(d) = d
            && let Some(l) = d.leader_addr
        {
            leader = Some(l);
            break;
        }
    }
    let Some(leader) = leader else {
        return;
    };
    if leader == cfg.advertise {
        return;
    }
    let m = call(node, cfg, &leader, |c| async move { c.members().await }).await;
    let _serial = node.transitions.lock().await;
    if node.role() != Role::Removed {
        return;
    }
    if let Ok(m) = m
        && let Some(theirs) = Members::from_wire(&pb::Members {
            epoch: m.members.epoch,
            version: m.members.version,
            addrs: m.members.addrs.clone(),
        })
        && theirs.generation() > node.members().generation()
    {
        let n = node.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || n.adopt_members(&theirs)).await {
            tracing::error!(error = %e, "members task");
        }
    }
    let connected = node.default_db().replica_status().connected;
    if (node.leader_addr().as_deref() != Some(leader.as_str()) || !connected)
        && let Err(e) = follow(node, &leader).await
    {
        tracing::debug!(error = %e.message(), "a removed node could not follow the leader");
    }
}

/// Runs the futures together and collects their outputs in order.
async fn futures_join<T, I, Fut>(futs: I) -> Vec<T>
where
    I: IntoIterator<Item = Fut>,
    Fut: std::future::Future<Output = T>,
{
    let mut pinned: Vec<std::pin::Pin<Box<Fut>>> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<T>> = (0..pinned.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut all = true;
        for (i, f) in pinned.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => out[i] = Some(v),
                    std::task::Poll::Pending => all = false,
                }
            }
        }
        if all {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    out.into_iter().map(|o| o.expect("completed")).collect()
}

/// What a vote round found.
struct Round {
    /// Members that granted.
    granted: usize,
    /// The first refusal that says this node is not up to date.
    behind: Option<String>,
    /// The highest membership a refusing peer answered with, when it
    /// is above the candidate's.
    newer_members: Option<Members>,
}

/// What a candidate asks for: the same in the pre-vote and the real
/// round, under the membership it currently holds.
struct Ballot<'a> {
    epoch: u64,
    data_epoch: u64,
    heads: &'a [pb::DatabaseHead],
    forced: bool,
}

/// One vote round to every other member of `members`.
async fn vote_round(
    node: &Arc<Node>,
    cfg: &ClusterConfig,
    members: &Members,
    ballot: &Ballot<'_>,
    pre_vote: bool,
) -> Round {
    let req = pb::RequestVoteRequest {
        epoch: ballot.epoch,
        candidate_addr: cfg.advertise.clone(),
        last_epoch: ballot.data_epoch,
        databases: ballot.heads.to_vec(),
        forced: ballot.forced,
        pre_vote,
        members: Some(members.to_wire()),
    };
    let targets = members.others(&cfg.advertise);
    let answers = futures_join(targets.iter().map(|peer| {
        let req = req.clone();
        async move {
            (
                peer.clone(),
                call(
                    node,
                    cfg,
                    peer,
                    |c| async move { c.request_vote(req).await },
                )
                .await,
            )
        }
    }))
    .await;
    let mut round = Round {
        granted: 0,
        behind: None,
        newer_members: None,
    };
    for (peer, answer) in answers {
        match answer {
            Ok(r) if r.granted => round.granted += 1,
            Ok(r) => {
                node.note_seen(r.epoch);
                tracing::info!(%peer, pre_vote, reason = %r.reason, "vote refused");
                if r.reason.starts_with("behind") && round.behind.is_none() {
                    round.behind = Some(format!("{peer}: {}", r.reason));
                }
                if let Some(theirs) = r.members.as_ref().and_then(Members::from_wire)
                    && theirs.generation() > members.generation()
                    && round
                        .newer_members
                        .as_ref()
                        .is_none_or(|m| theirs.generation() > m.generation())
                {
                    round.newer_members = Some(theirs);
                }
            }
            Err(e) => tracing::info!(%peer, pre_vote, error = %e, "vote request failed"),
        }
    }
    round
}

/// Campaigns to lead under the next epoch: a pre-vote round first
/// (nothing recorded anywhere), then, when a majority would grant, the
/// real one; a majority (this node's own vote counted, and recorded once
/// the round has won) promotes this node, with the votes as its first
/// lease, and tells the previous leader. `forced` (an operator's
/// Promote) does not wait for the silence timer: the live leader fences
/// itself and grants when the candidate holds everything it holds, and
/// fails the campaign otherwise; nodes that heard a leader within the
/// lease still refuse, so a forced campaign cannot overlap a live
/// leader it cannot reach.
pub async fn campaign(node: &Arc<Node>, forced: bool) -> Result<(u64, u64), Status> {
    let cfg = node.cluster.clone().ok_or_else(|| {
        Status::failed_precondition("this node has no peers; start it with --peer")
    })?;
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    let _serial = node.transitions.lock().await;
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    if node.role() == Role::Removed {
        return Err(node.refuse("not a member of the group"));
    }
    if !matches!(node.role(), Role::Replica | Role::Candidate | Role::Fenced) {
        return Err(node.refuse(format!("{:?}: already leading", node.role())));
    }
    // Re-checked under the lock: a vote granted while this task waited
    // for it is a promise this node must keep.
    if !forced && let Some((addr, e)) = node.heard_leader() {
        return Err(node.refuse(format!("heard {addr} (epoch {e}) within the lease")));
    }
    let epoch = node.floor_epoch() + 1;
    // The lowest epoch among the databases: a candidate claims an epoch
    // only when every database holds what that epoch's leader held.
    let data_epoch = node.vote_epoch();
    let heads: Vec<pb::DatabaseHead> = node
        .databases()
        .iter()
        .map(|d| pb::DatabaseHead {
            name: d.name.clone(),
            head: d.db.head(),
        })
        .collect();
    node.metrics.elections.inc();
    // The pre-vote round, repeated on a membership a peer is ahead with
    // (nothing is recorded there): the candidate adopts it and asks the
    // members it names.
    let mut members = node.members();
    let ballot = Ballot {
        epoch,
        data_epoch,
        heads: &heads,
        forced,
    };
    let mut pre = None;
    for _ in 0..3 {
        let r = vote_round(node, &cfg, &members, &ballot, true).await;
        match r.newer_members.clone() {
            Some(m) => {
                let n = node.clone();
                let m2 = m.clone();
                tokio::task::spawn_blocking(move || n.adopt_members(&m2))
                    .await
                    .map_err(|e| Status::internal(format!("members task: {e}")))??;
                if node.role() == Role::Removed {
                    return Err(node.refuse("not a member of the group (learned at the pre-vote)"));
                }
                members = m;
            }
            None => {
                pre = Some(r);
                break;
            }
        }
    }
    let Some(pre) = pre else {
        return Err(node.refuse("the membership kept changing during the pre-vote"));
    };
    if forced && let Some(b) = pre.behind {
        return Err(node.refuse(format!("not up to date: {b}")));
    }
    let quorum = members.quorum();
    if pre.granted + 1 < quorum {
        return Err(node.refuse(format!(
            "pre-vote at epoch {epoch}: {} of {quorum} needed",
            pre.granted + 1
        )));
    }
    let t0 = Instant::now();
    let real = vote_round(node, &cfg, &members, &ballot, false).await;
    if real.newer_members.is_some() {
        return Err(node.refuse("a peer is ahead on the membership; campaigning again later"));
    }
    if forced && let Some(b) = real.behind {
        return Err(node.refuse(format!("not up to date: {b}")));
    }
    if real.granted + 1 < quorum {
        return Err(node.refuse(format!(
            "no majority at epoch {epoch}: {} of {quorum} needed",
            real.granted + 1
        )));
    }
    let n = node.clone();
    let me = cfg.advertise.clone();
    tokio::task::spawn_blocking(move || n.record_vote(epoch, &me))
        .await
        .map_err(|e| Status::internal(format!("vote task: {e}")))??;
    let previous = node.heard_leader().map(|(addr, _)| addr);
    replica::stop_all(node).await;
    let n = node.clone();
    let result = tokio::task::spawn_blocking(move || {
        n.promote_locked(Some(epoch), Some(t0 + cfg.lease_ttl))
    })
    .await
    .map_err(|e| Status::internal(format!("promote task: {e}")))?;
    match result {
        Ok((epoch, head)) => {
            node.metrics.leader_changes.inc();
            heartbeat_round(node, &cfg).await;
            if let Some(addr) = previous.filter(|a| *a != cfg.advertise) {
                crate::service::spawn_fencer(node.clone(), epoch, cfg.upstream(&addr));
            }
            Ok((epoch, head))
        }
        Err(s) => {
            if node.role() == Role::Replica {
                replica::start_all(node).await;
            }
            Err(s)
        }
    }
}

/// Starts following `addr`: pullers stopped, the role and markers
/// switched, pullers started again towards the new leader.
async fn follow(node: &Arc<Node>, addr: &str) -> Result<(), Status> {
    replica::stop_all(node).await;
    let n = node.clone();
    let a = addr.to_string();
    tokio::task::spawn_blocking(move || n.become_replica_locked(&a))
        .await
        .map_err(|e| Status::internal(format!("follow task: {e}")))??;
    replica::start_all(node).await;
    Ok(())
}

/// Reconciles this node's membership with a sender's: a higher
/// generation is adopted (persisted first), a lower one is refused with
/// `Err(ours)`, an absent one (a sender before 0.5.0) changes nothing.
/// Under the transition lock.
async fn reconcile_members(node: &Arc<Node>, theirs: Option<&pb::Members>) -> Result<(), Members> {
    let Some(theirs) = theirs.and_then(Members::from_wire) else {
        return Ok(());
    };
    let ours = node.members();
    if theirs.generation() > ours.generation() {
        #[cfg(feature = "test-util")]
        if node
            .adopt_in_memory_only
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            node.install_members(theirs);
            return Ok(());
        }
        let n = node.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || n.adopt_members(&theirs)).await {
            tracing::error!(error = %e, "members task");
        }
        return Ok(());
    }
    if theirs.generation() < ours.generation() {
        return Err(ours);
    }
    Ok(())
}

/// Whether `addr` may lead or campaign here: a member of the membership
/// this node holds, or (for a heartbeat) the leader it already follows.
fn member_check(node: &Node, addr: &str, already_followed_ok: bool) -> bool {
    #[cfg(feature = "test-util")]
    if node
        .skip_member_check
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return true;
    }
    node.members().contains(addr)
        || (already_followed_ok
            && node.role().follows()
            && node.leader_addr().as_deref() == Some(addr))
}

/// A peer's heartbeat: acknowledged when its epoch is at least this
/// node's and at least the epoch it voted in, its membership is not
/// behind ours (a higher one is adopted first), and it is a member (or
/// the leader this node already follows). Acknowledging makes this node
/// a replica of that leader (a leader at an older epoch is fenced
/// first); a replica already following it just records the time.
pub async fn handle_heartbeat(
    node: &Arc<Node>,
    req: pb::HeartbeatRequest,
) -> Result<pb::HeartbeatResponse, Status> {
    if node.cluster.is_none() {
        return Err(Status::failed_precondition(
            "this node has no peers; start it with --peer",
        ));
    }
    if node.partitioned_from(&req.leader_addr) {
        return Err(Status::unavailable("partitioned (test)"));
    }
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    let _serial = node.transitions.lock().await;
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    let ttl = Duration::from_millis(req.lease_ttl_ms);
    let own = node.data_epoch();
    let answer = |ok: bool, node: &Node| pb::HeartbeatResponse {
        ok,
        epoch: node.floor_epoch(),
        leader_addr: node.leader_addr(),
        head: node.default_db().db.head(),
        members: Some(node.members().to_wire()),
    };
    if req.epoch < node.floor_epoch() {
        return Ok(answer(false, node));
    }
    if reconcile_members(node, req.members.as_ref()).await.is_err() {
        tracing::info!(leader = %req.leader_addr, "heartbeat from a leader behind on the membership; refused");
        return Ok(answer(false, node));
    }
    if !member_check(node, &req.leader_addr, true) {
        tracing::warn!(leader = %req.leader_addr, "heartbeat from an address outside the membership; refused");
        return Ok(answer(false, node));
    }
    match node.role() {
        Role::Leader => {
            if req.epoch == own {
                // Two leaders at one epoch cannot both hold a majority:
                // one of them is on a stale lease and refuses writes by
                // itself; refusing the ack keeps ours from extending.
                return Ok(answer(false, node));
            }
            let n = node.clone();
            let e = req.epoch;
            tokio::task::spawn_blocking(move || n.fence(e))
                .await
                .map_err(|e| Status::internal(format!("fence task: {e}")))??;
            node.metrics.leader_changes.inc();
            node.record_heard(&req.leader_addr, req.epoch, ttl);
            follow(node, &req.leader_addr).await?;
        }
        Role::Replica | Role::Removed
            if node.leader_addr().as_deref() == Some(req.leader_addr.as_str()) =>
        {
            node.record_heard(&req.leader_addr, req.epoch, ttl);
        }
        Role::Replica | Role::Removed | Role::Candidate | Role::Fenced => {
            node.record_heard(&req.leader_addr, req.epoch, ttl);
            follow(node, &req.leader_addr).await?;
        }
    }
    Ok(answer(true, node))
}

/// A peer's request for a vote. Granted when the epoch is new, this node
/// has not voted for another at that epoch, the candidate is at least as
/// up to date and no leader was heard within the lease. Up to date means:
/// a newer epoch; or the same epoch and a head at least ours on every
/// database this node has; when each is ahead on some database (pullers
/// of different databases progress independently) the larger total,
/// then the lower address, wins, so an election always ends. A leader
/// asked by force (an operator's Promote) checks the candidate first,
/// then fences itself and grants; every other node applies the recency
/// rule to a forced request too. A pre-vote answers without recording
/// anything.
pub async fn handle_vote(
    node: &Arc<Node>,
    req: pb::RequestVoteRequest,
) -> Result<pb::RequestVoteResponse, Status> {
    if node.cluster.is_none() {
        return Err(Status::failed_precondition(
            "this node has no peers; start it with --peer",
        ));
    }
    if node.partitioned_from(&req.candidate_addr) {
        return Err(Status::unavailable("partitioned (test)"));
    }
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    let _serial = node.transitions.lock().await;
    if node.is_stopping() {
        return Err(Status::unavailable("shutting down"));
    }
    let own = node.data_epoch();
    let floor = node.floor_epoch();
    let refuse = |reason: String| {
        Ok(pb::RequestVoteResponse {
            granted: false,
            epoch: floor,
            reason,
            members: Some(node.members().to_wire()),
        })
    };
    if req.epoch <= floor {
        return refuse(format!("stale: this node has promised epoch {floor}"));
    }
    if reconcile_members(node, req.members.as_ref()).await.is_err() {
        return refuse("membership: the candidate is behind this node's".into());
    }
    if node.role() == Role::Removed {
        return refuse("this node is not a member of the group".into());
    }
    if !member_check(node, &req.candidate_addr, false) {
        return refuse(format!(
            "not a member: {} is not in the membership (version {})",
            req.candidate_addr,
            node.members().version
        ));
    }
    if let Some((e, who)) = node.lease().voted
        && e == req.epoch
        && who != req.candidate_addr
    {
        return refuse(format!("already voted for {who} at epoch {e}"));
    }
    if req.last_epoch < own {
        return refuse(format!(
            "behind: the candidate is at epoch {}, this node at {own}",
            req.last_epoch
        ));
    }
    if req.last_epoch == own
        && let Some(reason) = behind(node, &cfg_advertise(node), &req)
    {
        return refuse(reason);
    }
    #[cfg(feature = "test-util")]
    let check_recency = !node
        .skip_recency_check
        .load(std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(feature = "test-util"))]
    let check_recency = true;
    let is_leader = node.role() == Role::Leader;
    // A forced request is exempt from recency only on the leader itself,
    // which fences before granting; every other node keeps its promise
    // to a leader it heard within the lease.
    if check_recency && !(req.forced && is_leader) {
        if is_leader {
            if node.lease_held() {
                return refuse("leading with a live lease".into());
            }
            let n = node.clone();
            let _ = tokio::task::spawn_blocking(move || n.step_down()).await;
            node.metrics.leader_changes.inc();
        }
        if let Some((addr, e)) = node.heard_leader() {
            return refuse(format!("heard {addr} (epoch {e}) within the lease"));
        }
    }
    if req.pre_vote {
        return Ok(pb::RequestVoteResponse {
            granted: true,
            epoch: floor,
            reason: String::new(),
            members: Some(node.members().to_wire()),
        });
    }
    if node.role() == Role::Leader {
        let n = node.clone();
        let e = req.epoch;
        tokio::task::spawn_blocking(move || n.fence(e))
            .await
            .map_err(|e| Status::internal(format!("fence task: {e}")))??;
        node.metrics.leader_changes.inc();
    }
    let n = node.clone();
    let (e, who) = (req.epoch, req.candidate_addr.clone());
    tokio::task::spawn_blocking(move || n.record_vote(e, &who))
        .await
        .map_err(|e| Status::internal(format!("vote task: {e}")))??;
    let ttl = node
        .cluster
        .as_ref()
        .map_or(Duration::ZERO, |c| c.lease_ttl);
    node.record_heard(&req.candidate_addr, req.epoch, ttl);
    tracing::info!(candidate = %req.candidate_addr, epoch = req.epoch, forced = req.forced, "vote granted");
    Ok(pb::RequestVoteResponse {
        granted: true,
        epoch: floor,
        reason: String::new(),
        members: Some(node.members().to_wire()),
    })
}

/// One membership change on the leader: validated, persisted as the
/// next generation, pushed with heartbeat rounds until committed or one
/// lease has passed (pending is not an error). Serialised by
/// `Node::member_changes`.
pub async fn change_members(
    node: &Arc<Node>,
    change: pb::change_members_request::Change,
    force: bool,
) -> Result<(Members, bool), Status> {
    let cfg = node.cluster.clone().ok_or_else(|| {
        Status::failed_precondition("this node has no peers; start it with --peer")
    })?;
    let _one = node.member_changes.lock().await;
    if node.role() != Role::Leader || !node.lease_held() {
        return Err(node
            .refuse("not the leader (or its lease expired); change the membership on the leader"));
    }
    let st = node.members_state();
    if !st.committed {
        let held = count_holding(node, &cfg, &st.current).await;
        return Err(Status::failed_precondition(format!(
            "a change to membership version {} is pending ({held} of {} members hold it); retry once `chron members` shows it committed",
            st.current.version,
            st.current.addrs.len()
        )));
    }
    let mut next = st.current.clone();
    next.epoch = node.data_epoch();
    next.version += 1;
    match change {
        pb::change_members_request::Change::Add(addr) => {
            let addr = valid_addr(&addr)?;
            if next.contains(&addr) {
                return Err(Status::already_exists(format!(
                    "{addr} is a member already"
                )));
            }
            if !force {
                let following = node
                    .default_db()
                    .sync
                    .snapshot(&st.current)
                    .iter()
                    .any(|f| f.peer == addr);
                if !following {
                    return Err(Status::failed_precondition(format!(
                        "{addr} is not following this leader on `default`; start it with --peer {} and wait for `chron status` to show it connected, or add it with force",
                        cfg.advertise
                    )));
                }
            }
            next.addrs.insert(addr);
        }
        pb::change_members_request::Change::Remove(addr) => {
            let addr = valid_addr(&addr)?;
            if !next.addrs.remove(&addr) {
                return Err(Status::not_found(format!("{addr} is not a member")));
            }
            if next.addrs.is_empty() {
                return Err(Status::failed_precondition(
                    "the membership cannot be emptied",
                ));
            }
        }
    }
    {
        // Under the transition lock, so no heartbeat handler adopts or
        // answers on a half-installed generation.
        let _serial = node.transitions.lock().await;
        if node.role() != Role::Leader || !node.lease_held() {
            return Err(node.refuse("not the leader any more"));
        }
        let n = node.clone();
        let m = next.clone();
        tokio::task::spawn_blocking(move || n.propose_members(&m))
            .await
            .map_err(|e| Status::internal(format!("members task: {e}")))??;
    }
    tracing::info!(members = %next.oneline(), "membership change proposed");
    // The new majority may exist already: the round decides.
    let deadline = Instant::now() + cfg.lease_ttl;
    loop {
        heartbeat_round(node, &cfg).await;
        let st = node.members_state();
        if st.committed || st.current.generation() != next.generation() {
            break;
        }
        if Instant::now() >= deadline || node.role() != Role::Leader {
            break;
        }
        tokio::select! {
            _ = node.cancel.cancelled() => break,
            _ = tokio::time::sleep(tick(cfg.lease_ttl)) => {}
        }
    }
    let st = node.members_state();
    Ok((st.current.clone(), st.committed))
}

/// `http://host:port` or `https://...`, no whitespace.
fn valid_addr(addr: &str) -> Result<String, Status> {
    let a = addr.trim();
    if a.is_empty()
        || a.chars().any(char::is_whitespace)
        || !(a.starts_with("http://") || a.starts_with("https://"))
    {
        return Err(Status::invalid_argument(format!(
            "{addr:?} is not an advertised address (http://host:port or https://host:port)"
        )));
    }
    Ok(a.to_string())
}

/// How many members of `m` answer a heartbeat at its generation right
/// now (for the pending-change message).
async fn count_holding(node: &Arc<Node>, cfg: &ClusterConfig, m: &Members) -> usize {
    let req = pb::HeartbeatRequest {
        epoch: node.data_epoch(),
        leader_addr: cfg.advertise.clone(),
        head: node.default_db().db.head(),
        lease_ttl_ms: ms(cfg.lease_ttl),
        members: Some(m.to_wire()),
    };
    let targets = m.others(&cfg.advertise);
    let answers = futures_join(targets.iter().map(|peer| {
        let req = req.clone();
        async move { call(node, cfg, peer, |c| async move { c.heartbeat(req).await }).await }
    }))
    .await;
    usize::from(m.contains(&cfg.advertise))
        + answers
            .iter()
            .filter(|a| a.as_ref().is_ok_and(|r| r.ok))
            .count()
}

fn cfg_advertise(node: &Node) -> String {
    node.cluster
        .as_ref()
        .map_or_else(String::new, |c| c.advertise.clone())
}

/// Why the candidate is behind this node at the same epoch, if it is:
/// a head below ours on any database this node has, unless the
/// candidate is ahead on another and wins the tie (larger total, then
/// the lower address).
fn behind(node: &Node, own_addr: &str, req: &pb::RequestVoteRequest) -> Option<String> {
    let mut ours_ahead: Vec<String> = Vec::new();
    let mut theirs_ahead = false;
    let (mut sum_ours, mut sum_theirs) = (0u64, 0u64);
    for d in node.databases() {
        let theirs = req
            .databases
            .iter()
            .find(|h| h.name == d.name)
            .map_or(0, |h| h.head);
        let ours = d.db.head();
        sum_ours += ours;
        sum_theirs += theirs;
        if theirs < ours {
            ours_ahead.push(format!(
                "{} is at {theirs} on the candidate, {ours} here",
                d.name
            ));
        } else if theirs > ours {
            theirs_ahead = true;
        }
    }
    if ours_ahead.is_empty() {
        return None;
    }
    if theirs_ahead
        && (sum_theirs > sum_ours
            || (sum_theirs == sum_ours && req.candidate_addr.as_str() < own_addr))
    {
        tracing::warn!(candidate = %req.candidate_addr, "each side is ahead on some database; the candidate wins the tie and what this node holds beyond it will be cut back");
        return None;
    }
    Some(format!("behind: {}", ours_ahead.join("; ")))
}

/// Who leads, as this node knows it: itself with a live lease, or the
/// leader heard within the lease (a manual replica names its upstream).
pub fn discover(node: &Node) -> pb::DiscoverResponse {
    let role = node.role();
    let leader_addr = match role {
        Role::Leader if node.lease_held() => Some(node.advertise()),
        Role::Leader => None,
        Role::Replica if node.cluster.is_none() => node.leader_addr(),
        // A removed node hears no heartbeats; it names the leader it
        // found and follows.
        Role::Removed => node.leader_addr(),
        Role::Replica | Role::Candidate | Role::Fenced => node.heard_leader().map(|(a, _)| a),
    };
    let members = node.members();
    pb::DiscoverResponse {
        leader_addr,
        epoch: node.data_epoch(),
        role: role.wire() as i32,
        peers: node
            .cluster
            .as_ref()
            .map_or_else(Vec::new, |c| members.others(&c.advertise)),
        lease_remaining_ms: (role == Role::Leader)
            .then(|| node.lease_remaining().map(ms))
            .flatten(),
        members_version: members.version,
        members_epoch: members.epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_between_a_tenth_and_a_half_of_the_ttl() {
        let ttl = Duration::from_secs(10);
        let mut j = Jitter::new("http://a:1");
        for _ in 0..1000 {
            let d = j.next(ttl);
            assert!(d >= ttl / 10 && d < ttl / 2, "{d:?}");
        }
    }

    #[test]
    fn the_tick_is_a_third_of_the_ttl_with_a_floor() {
        assert_eq!(tick(Duration::from_secs(9)), Duration::from_secs(3));
        assert_eq!(tick(Duration::from_millis(3)), Duration::from_millis(10));
    }
}
