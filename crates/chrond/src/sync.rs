//! Synchronous replication on a leader with peers: what each attached
//! follower has durably applied, the highest position a majority holds,
//! and the wait a sync commit makes for it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chron::Committed;
use chron_proto::v1 as pb;
use tokio::sync::watch;
use tonic::Status;

use crate::node::{DbNode, Members, Node, Role};
use crate::ops;

/// One follower attached through `Follow`.
#[derive(Debug, Clone)]
struct Follower {
    /// The stream this entry belongs to; a late report from an earlier
    /// stream of the same peer is ignored.
    id: u64,
    applied: u64,
    durable: bool,
    /// Whether it is at least as durable as the leader; its reports
    /// count only then, and only while it is in the membership.
    durable_ok: bool,
}

/// Per database on a leader: the attached followers and the
/// majority-applied position, published through a watch that only
/// rises (a position a majority once held stays held on disk). The
/// quorum and who counts come from the membership at each report.
pub struct SyncState {
    peers: Mutex<HashMap<String, Follower>>,
    majority: watch::Sender<u64>,
    next_id: AtomicU64,
}

impl Default for SyncState {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncState {
    pub fn new() -> SyncState {
        SyncState {
            peers: Mutex::new(HashMap::new()),
            majority: watch::Sender::new(0),
            next_id: AtomicU64::new(1),
        }
    }

    /// A new stream from `peer` replaces its earlier one; returns the
    /// stream's id, which every later report must carry.
    pub fn attach(&self, peer: &str, durable: bool, durable_ok: bool) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.peers.lock().unwrap_or_else(|p| p.into_inner()).insert(
            peer.to_string(),
            Follower {
                id,
                applied: 0,
                durable,
                durable_ok,
            },
        );
        id
    }

    /// `peer` reports `applied` on stream `id`; `own_head` is the
    /// leader's own, which counts as one durable copy when the leader is
    /// in `members`.
    pub fn record(
        &self,
        peer: &str,
        id: u64,
        applied: u64,
        own_head: u64,
        members: &Members,
        self_counted: bool,
    ) {
        {
            let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
            match peers.get_mut(peer) {
                Some(f) if f.id == id => f.applied = f.applied.max(applied),
                _ => return,
            }
        }
        self.publish(own_head, members, self_counted);
    }

    /// Recomputes the majority against `members` (after a membership
    /// change) and publishes it if it rose.
    pub fn publish(&self, own_head: u64, members: &Members, self_counted: bool) {
        let m = self.majority_for_head(own_head, members, self_counted);
        self.majority.send_if_modified(|cur| {
            if m > *cur {
                *cur = m;
                true
            } else {
                false
            }
        });
    }

    /// The highest position a majority of `members` durably holds, from
    /// the followers' reports and (when `self_counted`) `own_head`.
    pub fn majority_for_head(&self, own_head: u64, members: &Members, self_counted: bool) -> u64 {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let counted: Vec<u64> = peers
            .iter()
            .filter(|(addr, f)| f.durable_ok && members.contains(addr))
            .map(|(_, f)| f.applied)
            .collect();
        drop(peers);
        majority_applied(self_counted.then_some(own_head), &counted, members.quorum())
    }

    /// [`majority_for_head`](Self::majority_for_head) capped by the
    /// highest report, for a leader that does not count itself (one
    /// removing itself from the membership): what the followers alone
    /// hold.
    pub fn majority_for(&self, members: &Members, self_counted: bool) -> u64 {
        let own = *self.majority.borrow();
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let counted: Vec<u64> = peers
            .iter()
            .filter(|(addr, f)| f.durable_ok && members.contains(addr))
            .map(|(_, f)| f.applied)
            .collect();
        let cap = counted.iter().copied().max().unwrap_or(0).max(own);
        drop(peers);
        majority_applied(self_counted.then_some(cap), &counted, members.quorum())
    }

    /// The stream `id` of `peer` ended.
    pub fn detach(&self, peer: &str, id: u64) {
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        if peers.get(peer).is_some_and(|f| f.id == id) {
            peers.remove(peer);
        }
    }

    /// On promotion: nobody is attached and nothing is confirmed yet.
    pub fn reset(&self) {
        self.peers.lock().unwrap_or_else(|p| p.into_inner()).clear();
        self.majority.send_replace(0);
    }

    pub fn majority(&self) -> u64 {
        *self.majority.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.majority.subscribe()
    }

    /// Every attached follower, by address; `counted` as `members` says
    /// now.
    pub fn snapshot(&self, members: &Members) -> Vec<pb::FollowerStatus> {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<pb::FollowerStatus> = peers
            .iter()
            .map(|(peer, f)| pb::FollowerStatus {
                peer: peer.clone(),
                applied: f.applied,
                durable: f.durable,
                counted: f.durable_ok && members.contains(peer),
            })
            .collect();
        out.sort_by(|a, b| a.peer.cmp(&b.peer));
        out
    }
}

/// The quorum-th largest of `own_head` (when the leader counts) and the
/// counted followers' heads: the highest position at least `quorum`
/// copies hold. 0 while fewer than `quorum` copies exist.
pub fn majority_applied(own_head: Option<u64>, counted: &[u64], quorum: usize) -> u64 {
    let quorum = quorum.max(1);
    let mut heads: Vec<u64> = Vec::with_capacity(counted.len() + 1);
    // A follower cannot hold more than the leader; a report past the
    // leader's head is capped so a lying peer cannot confirm the future.
    let cap = own_head.unwrap_or(u64::MAX);
    heads.extend(own_head);
    heads.extend(counted.iter().map(|h| (*h).min(cap)));
    if heads.len() < quorum {
        return 0;
    }
    heads.sort_unstable_by(|a, b| b.cmp(a));
    heads[quorum - 1]
}

/// How a sync commit's wait ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waited {
    /// A majority reached the position after this long.
    Confirmed(Duration),
    TimedOut,
    /// The node stopped leading before a majority confirmed it.
    RoleLost(Role),
}

/// Waits until the database's majority-applied position reaches `next`,
/// at most `timeout`, or until the node stops leading. Called after the
/// write gate was released.
pub async fn wait_majority(node: &Node, dbn: &DbNode, next: u64, timeout: Duration) -> Waited {
    let mut rx = dbn.sync.subscribe();
    let mut role = node.watch_role();
    let start = Instant::now();
    tokio::select! {
        r = rx.wait_for(|m| *m >= next) => match r {
            Ok(_) => Waited::Confirmed(start.elapsed()),
            Err(_) => Waited::RoleLost(node.role()),
        },
        r = ops::role_lost(&mut role, true) => Waited::RoleLost(r),
        _ = tokio::time::sleep(timeout) => Waited::TimedOut,
    }
}

/// DEADLINE_EXCEEDED carrying where the transaction landed locally.
pub fn unconfirmed(c: &Committed, why: &str) -> Status {
    let mut s = Status::deadline_exceeded(format!(
        "committed locally at {}..{} but {why}; it stays on this node's disk and replicates unless the node dies; resend it with the same commit id, or check with an exact expectation or a read before resending without one",
        c.first, c.last
    ));
    for (key, v) in [
        (chron_proto::COMMITTED_FIRST_HEADER, c.first),
        (chron_proto::COMMITTED_NEXT_HEADER, c.next),
    ] {
        if let Ok(v) = v.to_string().parse() {
            s.metadata_mut().insert(key, v);
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(addrs: &[&str]) -> Members {
        Members {
            epoch: 1,
            version: 1,
            addrs: addrs.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn majority_is_the_quorum_th_largest_of_self_and_counted_peers() {
        assert_eq!(majority_applied(Some(100), &[100, 50], 2), 100);
        assert_eq!(majority_applied(Some(100), &[50], 2), 50);
        assert_eq!(
            majority_applied(Some(100), &[], 2),
            0,
            "fewer than a quorum"
        );
        assert_eq!(
            majority_applied(Some(100), &[120], 2),
            100,
            "capped by the leader"
        );
        assert_eq!(majority_applied(Some(100), &[], 1), 100, "a lone node");
        assert_eq!(majority_applied(Some(100), &[90, 80, 70, 60], 3), 80);
        assert_eq!(
            majority_applied(None, &[90, 80], 2),
            80,
            "a leader outside the membership does not count itself"
        );
    }

    #[test]
    fn uncounted_detached_stale_and_non_member_reports_do_not_move_the_majority() {
        let s = SyncState::new();
        let m = members(&["me", "a", "b"]);
        let a = s.attach("a", true, true);
        let b = s.attach("b", false, false);
        s.record("b", b, 100, 100, &m, true);
        assert_eq!(s.majority(), 0, "an uncounted follower proves nothing");
        s.record("a", a, 60, 100, &m, true);
        assert_eq!(s.majority(), 60);
        s.record("a", a, 50, 100, &m, true);
        assert_eq!(s.majority(), 60, "monotone");
        let a2 = s.attach("a", true, true);
        s.record("a", a, 500, 500, &m, true);
        assert_eq!(
            s.majority(),
            60,
            "a report from the replaced stream is ignored"
        );
        s.record("a", a2, 80, 100, &m, true);
        assert_eq!(s.majority(), 80);
        // Out of the membership, a's reports stop counting.
        let without_a = members(&["me", "b", "c"]);
        assert_eq!(s.majority_for_head(100, &without_a, true), 0);
        let c = s.attach("c", true, true);
        s.record("c", c, 70, 100, &without_a, true);
        assert_eq!(s.majority(), 80, "the watch never falls");
        assert_eq!(s.majority_for_head(100, &without_a, true), 70);
        assert_eq!(
            s.snapshot(&without_a)
                .iter()
                .map(|f| (f.peer.clone(), f.counted))
                .collect::<Vec<_>>(),
            [
                ("a".to_string(), false),
                ("b".to_string(), false),
                ("c".to_string(), true)
            ]
        );
        s.detach("a", a2);
        s.record("a", a2, 90, 100, &m, true);
        assert_eq!(s.snapshot(&m).len(), 2, "detached");
        s.reset();
        assert_eq!((s.majority(), s.snapshot(&m).len()), (0, 0));
    }
}
