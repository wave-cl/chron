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
//! leader's lease has expired; what can still be in flight is one commit
//! that passed the lease check an instant before. An elected leader's
//! votes are its first lease, measured the same way.
//!
//! A candidate records its own vote only once the round has won: the
//! campaign holds the node's transition lock, so no vote for another
//! candidate can slip in between, and a lost campaign leaves no promise
//! behind that would keep the node from following the live leader.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chron_client::ClientError;
use chron_proto::v1 as pb;
use tokio::task::JoinHandle;
use tonic::Status;

use crate::node::{ClusterConfig, Node, Role};
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
    // When the quiet period began, for a follower that has heard nothing.
    let mut quiet_since = Instant::now();
    loop {
        if node.cancel.is_cancelled() {
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
            Role::Fenced => {
                // Waits for the new leader's heartbeat; never campaigns.
                tokio::select! {
                    _ = node.cancel.cancelled() => return,
                    _ = tokio::time::sleep(tick(ttl)) => {}
                }
                quiet_since = Instant::now();
            }
            Role::Replica | Role::Candidate => {
                let heard_at = node.lease().heard.map(|(at, _, _)| at);
                let since = heard_at.unwrap_or(quiet_since);
                let deadline = since + ttl + jitter.next(ttl);
                let wait = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = node.cancel.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
                if node.heard_leader().is_some()
                    || !matches!(node.role(), Role::Replica | Role::Candidate)
                {
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
/// rarely campaign at once (xorshift seeded by the node's address).
struct Jitter(u64);

impl Jitter {
    fn new(seed: &str) -> Jitter {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in seed.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= Instant::now().elapsed().as_nanos() as u64;
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

/// One round of heartbeats to every peer. A majority (this node
/// included) extends the lease from the instant before the round; a
/// peer at a newer epoch fences this node.
pub async fn heartbeat_round(node: &Arc<Node>, cfg: &ClusterConfig) {
    let t0 = Instant::now();
    let epoch = node.data_epoch();
    let req = pb::HeartbeatRequest {
        epoch,
        leader_addr: cfg.advertise.clone(),
        head: node.default_db().db.head(),
        lease_ttl_ms: ms(cfg.lease_ttl),
    };
    let answers = futures_join(cfg.peers.iter().map(|peer| {
        let req = req.clone();
        async move {
            (
                peer.clone(),
                call(node, cfg, peer, |c| async move { c.heartbeat(req).await }).await,
            )
        }
    }))
    .await;
    let mut acks = 1;
    let mut newer: Option<u64> = None;
    for (peer, answer) in answers {
        match answer {
            Ok(r) if r.ok => acks += 1,
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
    if acks >= cfg.quorum() {
        node.extend_lease(t0 + cfg.lease_ttl);
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

/// One vote round at `epoch`; `(granted, behind)`: how many peers
/// granted, and the first refusal that says this node is not up to date.
async fn vote_round(
    node: &Arc<Node>,
    cfg: &ClusterConfig,
    epoch: u64,
    data_epoch: u64,
    heads: &[pb::DatabaseHead],
    forced: bool,
    pre_vote: bool,
) -> (usize, Option<String>) {
    let req = pb::RequestVoteRequest {
        epoch,
        candidate_addr: cfg.advertise.clone(),
        last_epoch: data_epoch,
        databases: heads.to_vec(),
        forced,
        pre_vote,
    };
    let answers = futures_join(cfg.peers.iter().map(|peer| {
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
    let mut granted = 0;
    let mut behind = None;
    for (peer, answer) in answers {
        match answer {
            Ok(r) if r.granted => granted += 1,
            Ok(r) => {
                node.note_seen(r.epoch);
                tracing::info!(%peer, pre_vote, reason = %r.reason, "vote refused");
                if r.reason.starts_with("behind") && behind.is_none() {
                    behind = Some(format!("{peer}: {}", r.reason));
                }
            }
            Err(e) => tracing::info!(%peer, pre_vote, error = %e, "vote request failed"),
        }
    }
    (granted, behind)
}

/// Campaigns to lead under the next epoch: one round of vote requests;
/// a majority (this node's own vote counted, and recorded once the round
/// has won) promotes this node, with the votes as its first lease, and
/// tells the previous leader. `forced` (an operator's Promote) ignores
/// leaders heard recently but fails if any peer says this node is
/// behind.
pub async fn campaign(node: &Arc<Node>, forced: bool) -> Result<(u64, u64), Status> {
    let cfg = node.cluster.clone().ok_or_else(|| {
        Status::failed_precondition("this node has no peers; start it with --peer")
    })?;
    let _serial = node.transitions.lock().await;
    if !matches!(node.role(), Role::Replica | Role::Candidate) {
        return Err(node.refuse(format!("{:?}: not a replica or candidate", node.role())));
    }
    let epoch = node.floor_epoch() + 1;
    let data_epoch = node.data_epoch();
    let heads: Vec<pb::DatabaseHead> = node
        .databases()
        .iter()
        .map(|d| pb::DatabaseHead {
            name: d.name.clone(),
            head: d.db.head(),
        })
        .collect();
    node.metrics.elections.inc();
    let t0 = Instant::now();
    let (granted, behind) = vote_round(node, &cfg, epoch, data_epoch, &heads, forced, false).await;
    if forced && let Some(b) = behind {
        return Err(node.refuse(format!("not up to date: {b}")));
    }
    if granted + 1 < cfg.quorum() {
        return Err(node.refuse(format!(
            "no majority at epoch {epoch}: {} of {} needed",
            granted + 1,
            cfg.quorum()
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

/// A peer's heartbeat: acknowledged when its epoch is at least this
/// node's and at least the epoch it voted in. Acknowledging makes this
/// node a replica of that leader (a leader at an older epoch is fenced
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
    let _serial = node.transitions.lock().await;
    let own = node.data_epoch();
    let answer = |ok: bool, node: &Node| pb::HeartbeatResponse {
        ok,
        epoch: node.floor_epoch(),
        leader_addr: node.leader_addr(),
        head: node.default_db().db.head(),
    };
    if req.epoch < node.floor_epoch() {
        return Ok(answer(false, node));
    }
    match node.role() {
        Role::Leader => {
            if req.epoch == own {
                // Two leaders at one epoch cannot both have a majority;
                // the other one learns our epoch and fences itself.
                return Ok(answer(false, node));
            }
            let n = node.clone();
            let e = req.epoch;
            tokio::task::spawn_blocking(move || n.fence(e))
                .await
                .map_err(|e| Status::internal(format!("fence task: {e}")))??;
            node.metrics.leader_changes.inc();
            node.record_heard(&req.leader_addr, req.epoch);
            follow(node, &req.leader_addr).await?;
        }
        Role::Replica if node.leader_addr().as_deref() == Some(req.leader_addr.as_str()) => {
            node.record_heard(&req.leader_addr, req.epoch);
        }
        Role::Replica | Role::Candidate | Role::Fenced => {
            node.record_heard(&req.leader_addr, req.epoch);
            follow(node, &req.leader_addr).await?;
        }
    }
    Ok(answer(true, node))
}

/// A peer's request for a vote. Granted when the epoch is new, this node
/// has not voted for another at that epoch, the candidate is at least as
/// up to date (epoch, then every database's head) and, unless `forced`,
/// no leader was heard within the lease. A leader asked by force checks
/// the candidate first, then fences itself and grants. A pre-vote
/// answers without recording anything.
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
    let _serial = node.transitions.lock().await;
    let own = node.data_epoch();
    let floor = node.floor_epoch();
    let refuse = |reason: String| {
        Ok(pb::RequestVoteResponse {
            granted: false,
            epoch: floor,
            reason,
        })
    };
    if req.epoch <= floor {
        return refuse(format!("stale: this node has promised epoch {floor}"));
    }
    if let Some((e, who)) = node.lease().voted
        && (e > req.epoch || (e == req.epoch && who != req.candidate_addr))
    {
        return refuse(format!("already voted for {who} at epoch {e}"));
    }
    if req.last_epoch < own {
        return refuse(format!(
            "behind: the candidate is at epoch {}, this node at {own}",
            req.last_epoch
        ));
    }
    if req.last_epoch == own {
        for d in node.databases() {
            let theirs = req
                .databases
                .iter()
                .find(|h| h.name == d.name)
                .map_or(0, |h| h.head);
            let ours = d.db.head();
            if theirs < ours {
                return refuse(format!(
                    "behind: {} is at {theirs} on the candidate, {ours} here",
                    d.name
                ));
            }
        }
    }
    #[cfg(feature = "test-util")]
    let check_recency = !node
        .skip_recency_check
        .load(std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(feature = "test-util"))]
    let check_recency = true;
    if !req.forced && check_recency {
        if node.role() == Role::Leader {
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
    node.record_heard(&req.candidate_addr, req.epoch);
    tracing::info!(candidate = %req.candidate_addr, epoch = req.epoch, forced = req.forced, "vote granted");
    Ok(pb::RequestVoteResponse {
        granted: true,
        epoch: floor,
        reason: String::new(),
    })
}

/// Who leads, as this node knows it.
pub fn discover(node: &Node) -> pb::DiscoverResponse {
    let role = node.role();
    let leader_addr = match role {
        Role::Leader if node.lease_held() => Some(node.advertise()),
        Role::Leader => None,
        Role::Replica => node.leader_addr(),
        Role::Candidate | Role::Fenced => node.heard_leader().map(|(a, _)| a),
    };
    pb::DiscoverResponse {
        leader_addr,
        epoch: node.data_epoch(),
        role: role.wire() as i32,
        peers: node
            .cluster
            .as_ref()
            .map_or_else(Vec::new, |c| c.peers.clone()),
        lease_remaining_ms: (role == Role::Leader)
            .then(|| node.lease_remaining().map(ms))
            .flatten(),
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
