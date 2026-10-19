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

use crate::node::{DbNode, Node, Role};
use crate::ops;

/// One follower attached through `Follow`.
#[derive(Debug, Clone)]
struct Follower {
    /// The stream this entry belongs to; a late report from an earlier
    /// stream of the same peer is ignored.
    id: u64,
    applied: u64,
    durable: bool,
    /// Whether its reports count toward the majority: a configured peer
    /// at least as durable as the leader.
    counted: bool,
}

/// Per database on a leader: the attached followers and the
/// majority-applied position, published through a watch that only
/// rises (a position a majority once held stays held on disk).
pub struct SyncState {
    quorum: usize,
    peers: Mutex<HashMap<String, Follower>>,
    majority: watch::Sender<u64>,
    next_id: AtomicU64,
}

impl SyncState {
    pub fn new(quorum: usize) -> SyncState {
        SyncState {
            quorum: quorum.max(1),
            peers: Mutex::new(HashMap::new()),
            majority: watch::Sender::new(0),
            next_id: AtomicU64::new(1),
        }
    }

    /// A new stream from `peer` replaces its earlier one; returns the
    /// stream's id, which every later report must carry.
    pub fn attach(&self, peer: &str, durable: bool, counted: bool) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.peers.lock().unwrap_or_else(|p| p.into_inner()).insert(
            peer.to_string(),
            Follower {
                id,
                applied: 0,
                durable,
                counted,
            },
        );
        id
    }

    /// `peer` reports `applied` on stream `id`; `own_head` is the
    /// leader's own, which counts as one durable copy.
    pub fn record(&self, peer: &str, id: u64, applied: u64, own_head: u64) {
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        match peers.get_mut(peer) {
            Some(f) if f.id == id => f.applied = f.applied.max(applied),
            _ => return,
        }
        let counted: Vec<u64> = peers
            .values()
            .filter(|f| f.counted)
            .map(|f| f.applied)
            .collect();
        drop(peers);
        let m = majority_applied(own_head, &counted, self.quorum);
        self.majority.send_if_modified(|cur| {
            if m > *cur {
                *cur = m;
                true
            } else {
                false
            }
        });
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

    /// Every attached follower, by address.
    pub fn snapshot(&self) -> Vec<pb::FollowerStatus> {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<pb::FollowerStatus> = peers
            .iter()
            .map(|(peer, f)| pb::FollowerStatus {
                peer: peer.clone(),
                applied: f.applied,
                durable: f.durable,
                counted: f.counted,
            })
            .collect();
        out.sort_by(|a, b| a.peer.cmp(&b.peer));
        out
    }
}

/// The quorum-th largest of `own_head` and the counted followers' heads:
/// the highest position at least `quorum` copies hold. 0 while fewer
/// than `quorum` copies exist.
pub fn majority_applied(own_head: u64, counted: &[u64], quorum: usize) -> u64 {
    let mut heads: Vec<u64> = Vec::with_capacity(counted.len() + 1);
    heads.push(own_head);
    // A follower cannot hold more than the leader; a report past the
    // leader's head is capped so a lying peer cannot confirm the future.
    heads.extend(counted.iter().map(|h| (*h).min(own_head)));
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

    #[test]
    fn majority_is_the_quorum_th_largest_of_self_and_counted_peers() {
        assert_eq!(majority_applied(100, &[100, 50], 2), 100);
        assert_eq!(majority_applied(100, &[50], 2), 50);
        assert_eq!(majority_applied(100, &[], 2), 0, "fewer than a quorum");
        assert_eq!(
            majority_applied(100, &[120], 2),
            100,
            "capped by the leader"
        );
        assert_eq!(majority_applied(100, &[], 1), 100, "a lone node");
        assert_eq!(majority_applied(100, &[90, 80, 70, 60], 3), 80);
    }

    #[test]
    fn uncounted_detached_and_stale_reports_do_not_move_the_majority() {
        let s = SyncState::new(2);
        let a = s.attach("a", true, true);
        let b = s.attach("b", false, false);
        s.record("b", b, 100, 100);
        assert_eq!(s.majority(), 0, "an uncounted follower proves nothing");
        s.record("a", a, 60, 100);
        assert_eq!(s.majority(), 60);
        s.record("a", a, 50, 100);
        assert_eq!(s.majority(), 60, "monotone");
        let a2 = s.attach("a", true, true);
        s.record("a", a, 500, 500);
        assert_eq!(
            s.majority(),
            60,
            "a report from the replaced stream is ignored"
        );
        s.record("a", a2, 80, 100);
        assert_eq!(s.majority(), 80);
        s.detach("a", a2);
        s.record("a", a2, 90, 100);
        assert_eq!(s.majority(), 80, "detached");
        assert_eq!(s.snapshot().len(), 1);
        s.reset();
        assert_eq!((s.majority(), s.snapshot().len()), (0, 0));
    }
}
