//! Automatic failover among peers: one leader at a time, elected by a
//! majority, with writes refused everywhere else.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chron_client::{Client, ClientError, Expected, NewEvent, Role as WireRole, Uuid};
use chron_proto::v1 as pb;
use chrond::node::Role;
use chrond::testing::Cluster;
use tokio_stream::StreamExt;

use crate::common::Server;

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);

async fn append(client: &Client, payload: &str) -> Result<u64, ClientError> {
    let mut t = chron_client::Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    client.commit(t).await.map(|c| c.first)
}

fn refused(r: &Result<u64, ClientError>) -> bool {
    matches!(r, Err(ClientError::Fenced { .. }))
}

async fn fill(client: &Client, n: usize) -> u64 {
    let mut last = 0;
    for i in 0..n {
        last = append(client, &format!("e{i}"))
            .await
            .expect("commit on the leader");
    }
    last + 1
}

#[tokio::test(flavor = "multi_thread")]
async fn three_peers_elect_one_leader_and_the_others_follow() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    let leader = c.client(l).await;
    let head = fill(&leader, 3).await;
    c.wait_heads(head, SOON).await;
    for i in 0..3 {
        let client = c.client(i).await;
        let d = client.discover().await.unwrap();
        assert_eq!(
            d.leader_addr.as_deref(),
            Some(c.addr(l).as_str()),
            "node {i}"
        );
        assert_eq!(d.peers.len(), 2);
        let info = client.info().await.unwrap();
        assert_eq!(info.leader_addr.as_deref(), Some(c.addr(l).as_str()));
        if i == l {
            assert_eq!(d.role, WireRole::Leader);
            assert!(info.lease_remaining.unwrap() > Duration::ZERO);
            assert!(d.lease_remaining.unwrap() > Duration::ZERO);
        } else {
            assert_eq!(d.role, WireRole::Replica);
            assert_eq!(info.replicating_from.as_deref(), Some(c.addr(l).as_str()));
            assert!(info.replica_connected, "node {i}");
            let r = append(&client, "nope").await;
            assert!(refused(&r), "node {i}: {r:?}");
        }
    }
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_leader_dies_and_a_replica_takes_over_then_the_old_leader_rejoins() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let head = fill(&c.client(l).await, 5).await;
    c.wait_heads(head, SOON).await;
    let epoch = c.node(l).data_epoch();

    let t_dead = Instant::now();
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let took = t_dead.elapsed();
    assert_ne!(l2, l);
    assert!(
        took < 3 * TTL + Duration::from_secs(1),
        "election took {took:?}"
    );
    assert!(c.node(l2).data_epoch() > epoch);
    let third = (0..3).find(|i| *i != l && *i != l2).unwrap();
    c.wait_following(third, l2, SOON).await;
    let new_leader = c.client(l2).await;
    let head = fill(&new_leader, 4).await;

    c.restart(l).await;
    c.wait_following(l, l2, SOON).await;
    c.wait_heads(head, SOON).await;
    let r = append(&c.client(l).await, "nope").await;
    assert!(refused(&r), "{r:?}");
    assert_eq!(c.leader(), Some(l2));
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_isolated_leader_refuses_writes_within_the_lease_and_the_majority_elects() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    let leader = c.client(l).await;
    fill(&leader, 2).await;
    let t_cut = Instant::now();
    c.partition(l);
    // Writes on the cut-off leader stop within one lease.
    let first_refusal = loop {
        let r = append(&leader, "x").await;
        if refused(&r) {
            break t_cut.elapsed();
        }
        assert!(
            t_cut.elapsed() < TTL + Duration::from_millis(400),
            "still accepting writes {:?} after the cut: {r:?}",
            t_cut.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(first_refusal <= TTL + Duration::from_millis(400));
    c.wait("the old leader a candidate", 3 * TTL, || {
        c.role(l) == Some(Role::Candidate)
    })
    .await;
    let l2 = c.wait_for_leader(SOON).await;
    assert_ne!(l2, l);
    let head = fill(&c.client(l2).await, 3).await;
    c.heal(l);
    c.wait_following(l, l2, SOON).await;
    c.wait_heads(head, SOON).await;
    c.stop_all().await;
}

/// Commits continuously on node `i`, recording when each success began
/// and ended.
fn probe(
    addr: String,
    i: usize,
    stop: Arc<AtomicBool>,
    successes: Arc<Mutex<Vec<(usize, Instant, Instant)>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let config = chron_client::ClientConfig {
            connect_timeout: Duration::from_millis(300),
            request_timeout: Some(Duration::from_millis(1500)),
            ..Default::default()
        };
        let mut client: Option<Client> = None;
        let mut n = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let Some(c) = &client else {
                match Client::connect(&addr, config.clone()).await {
                    Ok(c) => client = Some(c),
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
                continue;
            };
            let start = Instant::now();
            match append(c, &format!("p{i}-{n}")).await {
                Ok(_) => {
                    successes.lock().unwrap().push((i, start, Instant::now()));
                    n += 1;
                }
                Err(ClientError::Fenced { .. }) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(_) => {
                    client = None;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn at_most_one_node_accepts_writes_at_any_moment() {
    let mut c = Cluster::start(3, TTL).await;
    let stop = Arc::new(AtomicBool::new(false));
    let successes = Arc::new(Mutex::new(Vec::new()));
    let probes: Vec<_> = (0..3)
        .map(|i| probe(c.addr(i), i, stop.clone(), successes.clone()))
        .collect();
    let l = c.wait_for_leader(SOON).await;
    tokio::time::sleep(TTL).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    assert_ne!(l2, l);
    tokio::time::sleep(TTL).await;
    c.restart(l).await;
    c.wait_following(l, l2, SOON).await;
    tokio::time::sleep(TTL).await;
    c.partition(l2);
    let l3 = Cluster::wait_until_leader_other_than(&c, l2, SOON).await;
    tokio::time::sleep(TTL).await;
    c.heal(l2);
    c.wait_following(l2, l3, SOON).await;
    tokio::time::sleep(TTL).await;
    stop.store(true, Ordering::Relaxed);
    for p in probes {
        let _ = tokio::time::timeout(Duration::from_secs(5), p).await;
    }
    let all = successes.lock().unwrap().clone();
    // The third election may return the first leader, so two distinct
    // writers is the floor; the property is below.
    let writers: std::collections::BTreeSet<usize> = all.iter().map(|s| s.0).collect();
    assert!(writers.len() >= 2, "leadership never moved: {writers:?}");
    let mut overlaps = 0;
    for a in &all {
        for b in &all {
            if a.0 < b.0 && a.1 < b.2 && b.1 < a.2 {
                overlaps += 1;
            }
        }
    }
    assert_eq!(
        overlaps,
        0,
        "{overlaps} pairs of successful writes on different nodes overlapped in time ({} writes)",
        all.len()
    );
    c.stop_all().await;
}

impl ClusterExt for Cluster {}

trait ClusterExt {
    async fn wait_until_leader_other_than(c: &Cluster, not: usize, within: Duration) -> usize {
        let mut found = None;
        c.wait("another leader", within, || {
            found = c.leader().filter(|l| *l != not);
            found.is_some()
        })
        .await;
        found.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_survives_a_restart_and_a_deleted_marker_does_not() {
    for (delete_marker, want_granted) in [(false, false), (true, true)] {
        let mut c = Cluster::start(1, TTL).await;
        // One unreachable peer: the node can never win on its own.
        let o = c.options(0);
        c.stop(0).await;
        let _ = o;
        c.restart_with(0, |o| o.peers = vec!["http://127.0.0.1:1".into()])
            .await;
        assert_eq!(c.role(0), Some(Role::Candidate));
        let client = c.client(0).await;
        // The configured peer is the candidate: a vote for an address
        // outside the membership is refused.
        let peer = "http://127.0.0.1:1";
        let vote = |who: &str, pre_vote: bool| pb::RequestVoteRequest {
            epoch: 5,
            candidate_addr: who.to_string(),
            last_epoch: 5,
            databases: vec![],
            forced: true,
            pre_vote,
            members: None,
        };
        let r = client.request_vote(vote(peer, false)).await.unwrap();
        assert!(r.granted, "{r:?}");
        let marker = c.nodes[0].dir.path().join("data").join("vote");
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            format!("epoch 5 for {peer}\n")
        );
        c.stop(0).await;
        if delete_marker {
            std::fs::remove_file(&marker).unwrap();
        }
        c.restart_with(0, |o| o.peers = vec!["http://127.0.0.1:1".into()])
            .await;
        let client = c.client(0).await;
        // The same epoch asked again after the restart: the vote marker
        // says it is promised; without the marker it is granted afresh.
        let r = client.request_vote(vote(peer, true)).await.unwrap();
        assert_eq!(
            r.granted, want_granted,
            "delete_marker={delete_marker}: {r:?}"
        );
        if !delete_marker {
            assert!(r.reason.contains("promised epoch 5"), "{r:?}");
        }
        let hb_other = pb::HeartbeatRequest {
            epoch: 5,
            leader_addr: "http://y".into(),
            head: 0,
            lease_ttl_ms: 1000,
            members: None,
            confirmed: Vec::new(),
        };
        assert!(
            !client.heartbeat(hb_other).await.unwrap().ok,
            "a non-member's heartbeat is refused"
        );
        let r = client
            .request_vote(pb::RequestVoteRequest {
                candidate_addr: "http://y".into(),
                epoch: 6,
                ..vote(peer, true)
            })
            .await
            .unwrap();
        assert!(
            !r.granted && r.reason.contains("not a member"),
            "delete_marker={delete_marker}: a non-member cannot be voted for: {r:?}"
        );
        if !delete_marker {
            assert_eq!(client.info().await.unwrap().voted_epoch, Some(5));
            let hb = |epoch| pb::HeartbeatRequest {
                epoch,
                leader_addr: peer.into(),
                head: 0,
                lease_ttl_ms: 1000,
                members: None,
                confirmed: Vec::new(),
            };
            assert!(!client.heartbeat(hb(4)).await.unwrap().ok);
            assert!(client.heartbeat(hb(5)).await.unwrap().ok);
        }
        c.stop_all().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_peers_leads_alone_and_refuses_cluster_calls() {
    let s = Server::start().await;
    let client = Client::connect(s.addr(), Default::default()).await.unwrap();
    let d = client.discover().await.unwrap();
    assert_eq!(d.leader_addr.as_deref(), Some(s.addr().as_str()));
    assert_eq!(
        (d.role, d.peers.len(), d.lease_remaining),
        (WireRole::Leader, 0, None)
    );
    let info = client.info().await.unwrap();
    assert_eq!(
        (info.lease_remaining, info.peers.len(), info.voted_epoch),
        (None, 0, None)
    );
    let r = client
        .heartbeat(pb::HeartbeatRequest {
            epoch: 9,
            leader_addr: "http://x".into(),
            head: 0,
            lease_ttl_ms: 1000,
            members: None,
            confirmed: Vec::new(),
        })
        .await;
    assert!(
        matches!(r, Err(ClientError::FailedPrecondition(_))),
        "{r:?}"
    );
    let r = client
        .request_vote(pb::RequestVoteRequest {
            epoch: 9,
            candidate_addr: "http://x".into(),
            last_epoch: 9,
            databases: vec![],
            forced: true,
            pre_vote: true,
            members: None,
        })
        .await;
    assert!(
        matches!(r, Err(ClientError::FailedPrecondition(_))),
        "{r:?}"
    );
    assert_eq!(s.running.as_ref().unwrap().node.role(), Role::Leader);
    append(&client, "still writable").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_with_peers_hands_over_and_a_lagging_replica_is_refused() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    let (f1, f2) = (others[0], others[1]);
    for i in [f1, f2] {
        c.wait_following(i, l, SOON).await;
    }
    let leader = c.client(l).await;
    let head = fill(&leader, 5).await;
    c.wait_heads(head, SOON).await;

    // f2 misses three events, then comes back deaf to the leader: a
    // candidate that never catches up.
    c.stop(f2).await;
    let head = fill(&leader, 3).await;
    let leader_addr = c.addr(l);
    c.restart_with(f2, move |o| o.partition = vec![leader_addr])
        .await;
    assert_eq!(c.role(f2), Some(Role::Candidate));
    let r = c.client(f2).await.promote().await;
    match r {
        Err(ClientError::Fenced { message, .. }) => {
            assert!(message.contains("not up to date"), "{message}")
        }
        other => panic!("a lagging node was promoted: {other:?}"),
    }
    assert_eq!(c.leader(), Some(l));
    assert_eq!(
        c.node(f2).floor_epoch(),
        c.node(l).data_epoch(),
        "a lost campaign left a promise behind"
    );
    c.heal(f2);
    c.wait_following(f2, l, SOON).await;
    c.wait_heads(head, SOON).await;

    // f1 is up to date: the leader fences itself and follows.
    let p = c.client(f1).await.promote().await.unwrap();
    assert_eq!(p.head, head);
    assert_eq!(c.leader(), Some(f1));
    let r = append(&leader, "late").await;
    assert!(refused(&r), "the old leader still writes: {r:?}");
    c.wait_following(l, f1, SOON).await;
    c.wait_following(f2, f1, SOON).await;
    let head = fill(&c.client(f1).await, 2).await;
    c.wait_heads(head, SOON).await;
    c.stop_all().await;
}

/// A follower that lost its link to the leader, but not to the third
/// node, keeps campaigning; the third node refuses it because it heard
/// the leader within the lease. The control grants without that check:
/// the cut-off follower then wins, and two nodes lead at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_cut_off_from_the_leader_cannot_unseat_it() {
    for skip_recency_check in [false, true] {
        let mut c = Cluster::start(3, TTL).await;
        let l = c.wait_for_leader(SOON).await;
        let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
        let (f1, f2) = (others[0], others[1]);
        for i in [f1, f2] {
            c.wait_following(i, l, SOON).await;
        }
        let epoch = c.node(l).data_epoch();
        let floor = c.node(f2).floor_epoch();
        c.node(f1)
            .skip_recency_check
            .store(skip_recency_check, Ordering::Relaxed);
        c.cut(l, f2);
        tokio::time::sleep(3 * TTL).await;
        if skip_recency_check {
            assert_ne!(
                (c.role(l), c.node(l).data_epoch()),
                (Some(Role::Leader), epoch),
                "the control kept the leader in place:\n{}",
                c.describe()
            );
        } else {
            assert_eq!(c.leader(), Some(l), "{}", c.describe());
            assert_eq!(c.node(l).data_epoch(), epoch);
            assert_eq!(c.node(f2).floor_epoch(), floor, "{}", c.describe());
            let elections = c.node(f2).metrics_elections();
            assert!(
                (1..=4).contains(&elections),
                "the cut-off follower campaigned {elections} times in 3 s: it should wait a lease between campaigns"
            );
        }
        c.uncut(l, f2);
        let l = c.wait_for_leader(SOON).await;
        for i in (0..3).filter(|i| *i != l) {
            c.wait_following(i, l, SOON).await;
        }
        c.stop_all().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_at_start_campaigns() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let f1 = (0..3).find(|i| *i != l).unwrap();
    c.wait_following(f1, l, SOON).await;
    let head = fill(&c.client(l).await, 3).await;
    c.wait_heads(head, SOON).await;
    let epoch = c.node(l).data_epoch();
    c.stop(f1).await;
    c.restart_with(f1, |o| o.promote = true).await;
    c.wait("the promoted node leading", SOON, || c.leader() == Some(f1))
        .await;
    c.wait_following(l, f1, SOON).await;
    assert_eq!(c.node(f1).data_epoch(), epoch + 1);
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregates_keep_their_lengths_across_a_failover() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let id = Uuid::from_u128(7);
    let leader = c.client(l).await;
    for i in 0..4 {
        leader
            .append(id, Expected::Exact(i), vec![NewEvent::new("a")])
            .await
            .unwrap();
    }
    c.wait_heads(4, SOON).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let new_leader = c.client(l2).await;
    assert_eq!(new_leader.aggregate_len(id).await.unwrap(), 4);
    let r = new_leader
        .append(id, Expected::Exact(3), vec![NewEvent::new("stale")])
        .await;
    assert!(
        matches!(r, Err(ClientError::Conflict { actual: 4, .. })),
        "{r:?}"
    );
    new_leader
        .append(id, Expected::Exact(4), vec![NewEvent::new("b")])
        .await
        .unwrap();
    c.stop_all().await;
}

/// An old leader that comes back with writes nobody replicated cuts
/// itself back to the shared history, keeps the dropped tail as a backup
/// file, and follows the new leader; both when the new leader's head is
/// below the old one's and when it is past it.
#[tokio::test(flavor = "multi_thread")]
async fn a_diverged_old_leader_cuts_back_and_rejoins() {
    for extra_on_new_leader in [2usize, 6] {
        let mut c = Cluster::start(3, TTL).await;
        let l = c.wait_for_leader(SOON).await;
        for i in (0..3).filter(|i| *i != l) {
            c.wait_following(i, l, SOON).await;
        }
        let head = fill(&c.client(l).await, 5).await;
        c.wait_heads(head, SOON).await;
        let old_epoch = c.node(l).data_epoch();
        c.stop(l).await;
        // Three writes of its own, offline, as if they had been
        // acknowledged just before it died.
        let dir = c.nodes[l].dir.path().join("data").join("default");
        {
            let db =
                chron::Db::open(&dir, chron::Options::new().fsync(chron::Fsync::Never)).unwrap();
            for i in 0..3 {
                let mut t = db.transaction();
                t.append_free(vec![chron::NewEvent::new(format!("lost-{i}"))]);
                t.commit().unwrap();
            }
            assert_eq!(db.head(), 8);
        }
        let l2 = c.wait_for_leader(SOON).await;
        let new_head = fill(&c.client(l2).await, extra_on_new_leader).await;
        c.restart(l).await;
        c.wait_following(l, l2, SOON).await;
        c.wait("the old leader connected and caught up", SOON, || {
            let r = c.nodes[l].running.as_ref().unwrap();
            r.node.default_db().replica_status().connected && r.db.head() == new_head
        })
        .await;
        let (a, b) = (
            c.client(l).await.read_all(0, 0).await.unwrap(),
            c.client(l2).await.read_all(0, 0).await.unwrap(),
        );
        assert_eq!(
            a.iter().map(|e| e.payload.clone()).collect::<Vec<_>>(),
            b.iter().map(|e| e.payload.clone()).collect::<Vec<_>>()
        );
        let archives: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("diverged-"))
            .collect();
        assert_eq!(
            archives,
            [format!("diverged-{old_epoch}-5-8.chronbak")],
            "{archives:?}"
        );
        let meta = chron::inspect_backup(dir.join(&archives[0])).unwrap();
        assert_eq!((meta.base_head, meta.head), (Some(5), 8));
        assert_eq!(c.node(l).data_epoch(), c.node(l2).data_epoch());
        // It writes nothing of its own and is a full member again.
        let r = append(&c.client(l).await, "nope").await;
        assert!(refused(&r), "{r:?}");
        c.stop(l2).await;
        let l3 = c.wait_for_leader(SOON).await;
        assert_ne!(l3, l2);
        c.stop_all().await;
    }
}

/// A forced promotion of a node that lags, and cannot reach the leader,
/// fails at the pre-vote: no voter promises anything, so the leader,
/// which still holds a majority elsewhere, keeps leading. Without the
/// pre-vote the other cut-off follower's real vote would raise its floor
/// and fence the leader on its next heartbeat.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_forced_promote_leaves_no_promise_behind() {
    let mut c = Cluster::start(5, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..5).filter(|i| *i != l).collect();
    let (x, y) = (others[0], others[1]);
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    let leader = c.client(l).await;
    let head = fill(&leader, 5).await;
    c.wait_heads(head, SOON).await;
    let epoch = c.node(l).data_epoch();
    // Two followers come back deaf to the leader and miss three writes;
    // the other two keep acknowledging it.
    c.stop(x).await;
    c.stop(y).await;
    fill(&leader, 3).await;
    for i in [x, y] {
        let la = c.addr(l);
        c.restart_with(i, move |o| o.partition = vec![la]).await;
    }
    let r = c.client(x).await.promote().await;
    assert!(matches!(&r, Err(ClientError::Fenced { .. })), "{r:?}");
    tokio::time::sleep(2 * TTL).await;
    assert_eq!(c.leader(), Some(l), "{}", c.describe());
    assert_eq!(c.node(l).data_epoch(), epoch);
    assert!(
        c.node(y).lease().voted.map(|(e, _)| e).unwrap_or(0) <= epoch,
        "the other cut-off follower promised an epoch to the refused candidate"
    );
    c.heal(x);
    c.heal(y);
    for i in [x, y] {
        c.wait_following(i, l, SOON).await;
    }
    c.stop_all().await;
}

/// A leader that runs a longer lease than its followers is waited for
/// that long: the followers do not elect another leader between its
/// heartbeats.
#[tokio::test(flavor = "multi_thread")]
async fn followers_wait_out_a_leaders_longer_lease() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in (0..3).filter(|i| *i != l) {
        c.wait_following(i, l, SOON).await;
    }
    // The leader restarts with a lease six times longer (heartbeats
    // every 2 s) and takes the lead back by force.
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    c.restart_with(l, |o| o.lease_ttl = 6 * TTL).await;
    c.wait_following(l, l2, SOON).await;
    c.client(l).await.promote().await.unwrap();
    c.wait("the long-lease node leading", SOON, || {
        c.leader() == Some(l)
    })
    .await;
    let epoch = c.node(l).data_epoch();
    tokio::time::sleep(4 * TTL).await;
    assert_eq!(c.leader(), Some(l), "{}", c.describe());
    assert_eq!(
        c.node(l).data_epoch(),
        epoch,
        "a follower elected a new leader between heartbeats"
    );
    c.stop_all().await;
}

/// A node fenced by an epoch whose leader never reaches it is not stuck:
/// after a lease of silence it campaigns like a candidate. Here it holds
/// writes the one other live node lacks, so that node cannot be elected
/// and nobody rescues the fenced node with a heartbeat: only its own
/// campaign gives the group a leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_node_campaigns_after_silence() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    let head = fill(&c.client(l).await, 3).await;
    c.wait_heads(head, SOON).await;
    let epoch = c.node(l).data_epoch();
    for i in &others {
        c.stop(*i).await;
    }
    // Writes the others never see, landed through the library handle
    // while the leader's lease runs out.
    {
        let db = c.nodes[l].running.as_ref().unwrap().db.clone();
        for i in 0..3 {
            let mut t = db.transaction();
            t.append_free(vec![chron::NewEvent::new(format!("only-here-{i}"))]);
            t.commit().unwrap();
        }
    }
    // A fence by an epoch nobody leads under.
    c.client(l).await.fence(epoch + 1).await.unwrap();
    assert_eq!(c.role(l), Some(Role::Fenced));
    // One follower comes back: it is behind, so the fenced node refuses
    // its votes, and with the third node down a majority needs the fenced
    // node's own campaign.
    let f = others[0];
    c.restart(f).await;
    c.wait("the fenced node leading again", 3 * SOON, || {
        c.leader() == Some(l)
    })
    .await;
    assert!(c.node(l).data_epoch() > epoch + 1);
    c.wait_following(f, l, SOON).await;
    c.wait_heads(head + 3, SOON).await;
    c.stop_all().await;
}

/// With peers a writer's epoch token cannot unseat the leader: an epoch
/// one above is refused like any other wrong epoch.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_epoch_one_above_does_not_fence_a_group_leader() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let epoch = c.node(l).data_epoch();
    let client = c.client(l).await;
    let mut t = chron_client::Transaction::new();
    t.append_free(vec![NewEvent::new("x")]);
    let r = client.commit_with_epoch(t, epoch + 1).await;
    assert!(matches!(r, Err(ClientError::Fenced { .. })), "{r:?}");
    assert_eq!(c.role(l), Some(Role::Leader));
    assert_eq!(c.leader(), Some(l));
    append(&client, "still writable").await.unwrap();
    c.stop_all().await;
}

/// Opens the leader's `default` database offline with tiny segments,
/// writes `n` more events and retains past `floor`, so a lagging node
/// has nothing to pull from.
fn retain_offline(dir: &std::path::Path, n: usize, floor: u64) -> u64 {
    let db = chron::Db::open(
        dir,
        chron::Options::new()
            .fsync(chron::Fsync::Never)
            .segment_max_bytes(1),
    )
    .unwrap();
    for i in 0..n {
        let mut t = db.transaction();
        t.append_free(vec![chron::NewEvent::new(format!("later-{i}"))]);
        t.commit().unwrap();
    }
    let r = db.retain_from(floor).unwrap();
    assert!(r.floor >= floor / 2 && r.floor > 0, "{r:?}");
    r.floor
}

fn restores(c: &Cluster, i: usize, result: &str) -> u64 {
    c.node(i)
        .metrics
        .restores
        .with_label_values(&["default", result])
        .get()
}

fn replaced_dirs(c: &Cluster, i: usize) -> Vec<String> {
    std::fs::read_dir(c.nodes[i].dir.path().join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("default.replaced-") || n == "default.fetched")
        .collect()
}

/// A replica whose head lies below the leader's retention floor, stale or
/// empty, fetches the leader's backup and takes it as its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_below_the_leaders_floor_restores_itself() {
    for empty in [false, true] {
        let mut c = Cluster::start(3, TTL).await;
        let l = c.wait_for_leader(SOON).await;
        let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
        for i in &others {
            c.wait_following(*i, l, SOON).await;
        }
        let head = fill(&c.client(l).await, 5).await;
        c.wait_heads(head, SOON).await;
        c.stop_all().await;
        let floor = retain_offline(
            &c.nodes[l].dir.path().join("data").join("default"),
            195,
            100,
        );
        let (f1, f2) = (others[0], others[1]);
        if empty {
            std::fs::remove_dir_all(c.nodes[f2].dir.path().join("data")).unwrap();
        }
        c.restart(l).await;
        c.restart(f1).await;
        // The leader is ahead, so it wins; f1 at head 5 has nothing to
        // pull and restores itself.
        c.wait("the retained node leading", 3 * SOON, || {
            c.leader() == Some(l)
        })
        .await;
        // A subscription on f1 opened before the swap ends saying so.
        let early = c.client(f1).await;
        let mut sub = early.subscribe(0).await.unwrap();
        c.restart(f2).await;
        c.wait("both restored", 3 * SOON, || {
            restores(&c, f1, "ok") == 1 && restores(&c, f2, "ok") == 1
        })
        .await;
        for i in [f1, f2] {
            c.wait_following(i, l, SOON).await;
        }
        c.wait_heads(200, SOON).await;
        for i in [f1, f2] {
            // The copy already holds the head; its puller connects after.
            c.wait(&format!("node {i} connected"), SOON, || {
                c.node(i).default_db().replica_status().connected
            })
            .await;
            let info = c.client(i).await.info().await.unwrap();
            assert_eq!((info.head, info.floor), (200, floor), "node {i}");
            assert!(info.replica_connected);
            let (a, b) = (
                c.client(i)
                    .await
                    .read_all_with(floor, 0, chrond::testing::local_read())
                    .await
                    .unwrap(),
                c.client(l)
                    .await
                    .read_all_with(floor, 0, chrond::testing::local_read())
                    .await
                    .unwrap(),
            );
            assert_eq!(
                a.iter().map(|e| e.payload.clone()).collect::<Vec<_>>(),
                b.iter().map(|e| e.payload.clone()).collect::<Vec<_>>()
            );
            assert_eq!(c.node(i).data_epoch(), c.node(l).data_epoch());
        }
        let ended = loop {
            match tokio::time::timeout(SOON, sub.next()).await {
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(e))) => break e,
                other => panic!("the early subscription did not end: {other:?}"),
            }
        };
        assert!(
            ended.to_string().contains("restored from the leader"),
            "{ended}"
        );
        let kept = replaced_dirs(&c, f1);
        assert_eq!(kept.len(), 1, "{kept:?}");
        assert!(kept[0].starts_with("default.replaced-"), "{kept:?}");
        assert!(
            replaced_dirs(&c, f2).is_empty() == empty,
            "{:?}",
            replaced_dirs(&c, f2)
        );
        let head = fill(&c.client(l).await, 2).await;
        c.wait_heads(head, SOON).await;
        c.stop(l).await;
        let l2 = c.wait_for_leader(SOON).await;
        assert_ne!(l2, l);
        c.stop_all().await;
        // The kept copy is the old database, head 5.
        let kept_db = chron::Db::open(
            c.nodes[f1].dir.path().join("data").join(&kept[0]),
            chron::Options::new().fsync(chron::Fsync::Never),
        )
        .unwrap();
        assert_eq!(kept_db.head(), 5);
    }
}

/// An old leader whose own writes lie entirely below the new leader's
/// floor cannot cut back to a shared history: it restores itself and
/// keeps its old copy, writes included, aside.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_shared_history_lies_below_the_floor_restores_itself() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    let head = fill(&c.client(l).await, 5).await;
    c.wait_heads(head, SOON).await;
    c.stop_all().await;
    // The old leader's own tail: 145 events nobody saw.
    {
        let db = chron::Db::open(
            c.nodes[l].dir.path().join("data").join("default"),
            chron::Options::new().fsync(chron::Fsync::Never),
        )
        .unwrap();
        for i in 0..145 {
            let mut t = db.transaction();
            t.append_free(vec![chron::NewEvent::new(format!("lost-{i}"))]);
            t.commit().unwrap();
        }
        assert_eq!(db.head(), 150);
    }
    let (f1, f2) = (others[0], others[1]);
    let floor = retain_offline(
        &c.nodes[f1].dir.path().join("data").join("default"),
        115,
        100,
    );
    c.restart(f1).await;
    c.restart(f2).await;
    c.wait("the retained node leading", 3 * SOON, || {
        c.leader() == Some(f1)
    })
    .await;
    c.restart(l).await;
    c.wait("the old leader restored", 3 * SOON, || {
        restores(&c, l, "ok") == 1
    })
    .await;
    c.wait_following(l, f1, SOON).await;
    c.wait_heads(120, SOON).await;
    let info = c.client(l).await.info().await.unwrap();
    assert_eq!((info.head, info.floor), (120, floor));
    let kept = replaced_dirs(&c, l);
    assert_eq!(kept.len(), 1, "{kept:?}");
    c.stop_all().await;
    let kept_db = chron::Db::open(
        c.nodes[l].dir.path().join("data").join(&kept[0]),
        chron::Options::new().fsync(chron::Fsync::Never),
    )
    .unwrap();
    assert_eq!(kept_db.head(), 150);
    assert_eq!(kept_db.read_all(149, 1).unwrap()[0].payload, "lost-144");
}

/// A fetched backup of another database is refused and the old copy stays.
#[tokio::test(flavor = "multi_thread")]
async fn a_fetched_backup_of_another_database_is_refused_and_the_old_copy_stays() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    let leader = c.client(l).await;
    leader.create_database("orders").await.unwrap();
    let head = fill(&leader, 5).await;
    c.wait_heads(head, SOON).await;
    c.stop_all().await;
    retain_offline(
        &c.nodes[l].dir.path().join("data").join("default"),
        195,
        100,
    );
    let f1 = others[0];
    c.restart(l).await;
    c.node(l)
        .fetch_alias
        .lock()
        .unwrap()
        .insert("default".into(), "orders".into());
    c.restart(f1).await;
    c.wait("the retained node leading", 3 * SOON, || {
        c.leader() == Some(l)
    })
    .await;
    c.wait("the wrong backup refused", 3 * SOON, || {
        restores(&c, f1, "error") >= 1
    })
    .await;
    // The puller records the refusal, then goes on trying; catch it.
    c.wait("the refusal in last_error", SOON, || {
        c.node(f1)
            .default_db()
            .replica_status()
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("is of database"))
    })
    .await;
    assert_eq!(c.node(f1).default_db().db.head(), 5);
    assert!(
        replaced_dirs(&c, f1).is_empty(),
        "{:?}",
        replaced_dirs(&c, f1)
    );
    c.node(l).fetch_alias.lock().unwrap().clear();
    c.wait("restored once the alias is gone", 3 * SOON, || {
        restores(&c, f1, "ok") == 1
    })
    .await;
    c.wait_following(f1, l, SOON).await;
    c.wait_heads(200, SOON).await;
    c.stop_all().await;
}

/// A commit resent under its id after the leader died lands exactly
/// once: as a duplicate when the write had replicated, afresh when it
/// died with the leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_id_lands_once_across_a_leader_death() {
    let mut c = Cluster::start(3, TTL).await;
    let l1 = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l1).collect();
    for i in &others {
        c.wait_following(*i, l1, SOON).await;
    }
    c.wait("followers attached", SOON, || c.followers(l1).len() == 2)
        .await;
    let txn = |payload: &str| {
        let mut t = chron_client::Transaction::new();
        t.append_free(vec![NewEvent::new(payload.to_string())]);
        t
    };
    let with_id = |id: u8| chron_client::CommitOptions {
        commit_id: Some([id; 16]),
        ..Default::default()
    };
    // (a) Replicated, then the leader dies: the resend is a duplicate.
    let a = c
        .client(l1)
        .await
        .commit_with(txn("replicated"), with_id(1))
        .await
        .unwrap();
    assert_eq!((a.first, a.duplicate), (0, false));
    c.wait_heads(1, SOON).await;
    c.stop(l1).await;
    let l2 = c.wait_for_leader(SOON).await;
    let again = c
        .client(l2)
        .await
        .commit_with(txn("replicated"), with_id(1))
        .await
        .unwrap();
    assert_eq!((again.first, again.next, again.duplicate), (0, 1, true));
    // (b) Held back on the follower, so it dies with the leader: the
    // resend lands afresh on the next leader.
    let f = (0..3).find(|i| *i != l1 && *i != l2).unwrap();
    c.restart(l1).await;
    c.wait_following(l1, l2, SOON).await;
    c.wait("followers attached", SOON, || c.followers(l2).len() == 2)
        .await;
    c.hold_apply(f, true);
    c.hold_apply(l1, true);
    let client = c.client(l2).await;
    let r = client
        .commit_with(
            txn("lost"),
            chron_client::CommitOptions {
                sync: Some(false),
                ..with_id(2)
            },
        )
        .await
        .unwrap();
    assert_eq!((r.first, r.duplicate), (1, false));
    assert_eq!(c.node(l2).default_db().db.head(), 2);
    // The holds stay through the election: a released follower would
    // apply the chunk it already received.
    c.stop(l2).await;
    let l3 = c.wait_for_leader(SOON).await;
    assert_ne!(l3, l2);
    assert_eq!(
        c.node(l3).default_db().db.head(),
        1,
        "the write died with {l2}"
    );
    let fresh = c
        .client(l3)
        .await
        .commit_with(
            txn("lost"),
            chron_client::CommitOptions {
                sync: Some(false),
                ..with_id(2)
            },
        )
        .await
        .unwrap();
    assert_eq!((fresh.first, fresh.duplicate), (1, false));
    c.hold_apply(f, false);
    c.hold_apply(l1, false);
    let log = c
        .client(l3)
        .await
        .read_all_with(0, 0, chrond::testing::local_read())
        .await
        .unwrap();
    let lost: Vec<_> = log.iter().filter(|e| e.payload == "lost").collect();
    assert_eq!(lost.len(), 1, "exactly one copy");
    assert_eq!(log.iter().filter(|e| e.payload == "replicated").count(), 1);
    c.stop_all().await;
}

/// A commit that waited in the queue past the leader's lease is refused
/// right before it would land, instead of landing under a stale lease.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_queued_past_the_lease_is_refused_before_landing() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    let db = c.node(l).default_db().db.clone();
    let head = db.head();
    // The writer is held: the commit queues behind it.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
    let holder = {
        let db = db.clone();
        std::thread::spawn(move || {
            let hold = db.debug_hold_writer();
            held_tx.send(()).unwrap();
            let _ = release_rx.recv();
            drop(hold);
        })
    };
    held_rx.recv().unwrap();
    let client = c.client_patient(l).await;
    let mut late = chron_client::Transaction::new();
    late.append_free(vec![NewEvent::new("late".to_string())]);
    // Spawned, so the request is sent while this task waits.
    let pending = tokio::spawn(async move {
        client
            .commit_with(
                late,
                chron_client::CommitOptions {
                    sync: Some(false),
                    ..Default::default()
                },
            )
            .await
    });
    Cluster::wait_until("the commit queued", SOON, || {
        db.debug_pending_commits() == 1
    })
    .await;
    assert!(
        !pending.is_finished(),
        "the commit waits behind the held writer"
    );
    // The lease runs out while it waits: no peer answers from now on,
    // and the lease is expired by hand so the test does not wait a ttl.
    c.partition(l);
    c.node(l)
        .with_lease(|lease| lease.held_until = Some(Instant::now() - Duration::from_millis(1)));
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    match tokio::time::timeout(SOON, pending).await.unwrap().unwrap() {
        Err(ClientError::Fenced { message, .. }) => {
            assert!(
                message.contains("expired before the write landed"),
                "{message}"
            )
        }
        other => panic!("landed or another error: {other:?}"),
    }
    assert_eq!(db.head(), head, "nothing of it was written");
    c.heal(l);
    c.stop_all().await;
}

/// A tombstone recorded on the leader reaches the followers, so the
/// next leader still answers it after the first one died.
#[tokio::test(flavor = "multi_thread")]
async fn a_tombstone_survives_the_leaders_death() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.discover_interval = Duration::from_secs(1);
    })
    .await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    let client = c.client(l).await;
    let tmp = client.create_database("tmp").await.unwrap();
    // A write, so every mirror attached and took the leader's identity
    // (a tombstone names the identity, not the name).
    let mut t = chron_client::Transaction::new();
    t.append_free(vec![NewEvent::new("x".to_string())]);
    // Landed locally: the mirrors are not attached yet, so a sync
    // commit would wait for them first.
    client
        .database("tmp")
        .commit_with(
            t,
            chron_client::CommitOptions {
                sync: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    c.wait("the mirrors of tmp", SOON, || {
        (0..3).all(|i| {
            c.node(i)
                .get("tmp")
                .is_some_and(|d| d.db.head() == 1 && d.db.id() == tmp.db_id)
        })
    })
    .await;
    client.drop_database("tmp").await.unwrap();
    c.wait("the followers to drop tmp", SOON, || {
        (0..3).all(|i| c.node(i).get("tmp").is_none())
    })
    .await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let listing = c.client(l2).await.list_databases_full().await.unwrap();
    assert!(
        listing
            .dropped
            .iter()
            .any(|t| t.db_id == tmp.db_id && t.name == "tmp"),
        "{listing:?}"
    );
    c.stop_all().await;
}
