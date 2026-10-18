//! Automatic failover among peers: one leader at a time, elected by a
//! majority, with writes refused everywhere else.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chron_client::{Client, ClientError, Expected, NewEvent, Role as WireRole, Uuid};
use chron_proto::v1 as pb;
use chrond::node::Role;
use chrond::testing::Cluster;

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
        let vote = |who: &str, pre_vote: bool| pb::RequestVoteRequest {
            epoch: 5,
            candidate_addr: who.to_string(),
            last_epoch: 5,
            databases: vec![],
            forced: true,
            pre_vote,
        };
        let r = client.request_vote(vote("http://x", false)).await.unwrap();
        assert!(r.granted, "{r:?}");
        let marker = c.nodes[0].dir.path().join("data").join("vote");
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            "epoch 5 for http://x\n"
        );
        c.stop(0).await;
        if delete_marker {
            std::fs::remove_file(&marker).unwrap();
        }
        c.restart_with(0, |o| o.peers = vec!["http://127.0.0.1:1".into()])
            .await;
        let client = c.client(0).await;
        let r = client.request_vote(vote("http://y", true)).await.unwrap();
        assert_eq!(
            r.granted, want_granted,
            "delete_marker={delete_marker}: {r:?}"
        );
        if !delete_marker {
            assert!(r.reason.contains("promised epoch 5"), "{r:?}");
            assert_eq!(client.info().await.unwrap().voted_epoch, Some(5));
            let hb = |epoch| pb::HeartbeatRequest {
                epoch,
                leader_addr: "http://x".into(),
                head: 0,
                lease_ttl_ms: 1000,
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
            assert!(
                c.node(f2).metrics_elections() > 0,
                "the cut-off follower never campaigned"
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
        // The epoch in the name is the old leader's, whatever the
        // initial election took.
        assert_eq!(archives.len(), 1, "{archives:?}");
        assert!(
            archives[0].starts_with("diverged-") && archives[0].ends_with("-5-8.chronbak"),
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
