//! Dynamic membership: nodes added to and removed from a failover group
//! at runtime, one at a time, through the leader.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chron_client::{Client, ClientError, NewEvent, Role as WireRole, Transaction};
use chron_proto::v1 as pb;
use chrond::node::Role;
use chrond::testing::Cluster;

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);

fn free(payload: &str) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    t
}

/// A leader, every other node following it and attached.
async fn settled(c: &Cluster) -> (usize, Vec<usize>) {
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..c.nodes.len()).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    c.wait("every follower attached", SOON, || {
        c.followers(l).len() == others.len()
    })
    .await;
    let epoch = c.node(l).data_epoch();
    c.wait("every follower at the leader's epoch", SOON, || {
        others.iter().all(|i| c.node(*i).data_epoch() == epoch)
    })
    .await;
    c.wait("the leader's membership committed", SOON, || {
        c.members_committed(l)
    })
    .await;
    (l, others)
}

/// Waits until the removed node `i` follows the leader `l` with a live
/// stream (its Follow is attached on the leader).
async fn wait_removed_following(c: &Cluster, i: usize, l: usize) {
    c.wait_removed(i, SOON).await;
    let addr = c.addr(i);
    c.wait(&format!("node {i} attached to the leader"), SOON, || {
        c.followers(l).iter().any(|f| f.peer == addr)
    })
    .await;
}

async fn wait_committed(c: &Cluster, l: usize) {
    c.wait("the change committed", SOON, || c.members_committed(l))
        .await;
}

fn members_file(c: &Cluster, i: usize) -> String {
    std::fs::read_to_string(c.nodes[i].dir.path().join("data").join("members")).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fourth_node_is_added_follows_and_counts_for_sync_commits() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let v1 = c.members(l).version;
    let d = c.add_node(l).await;
    wait_removed_following(&c, d, l).await;
    let client = c.client(l).await;
    assert!(
        !c.followers(l)
            .iter()
            .any(|f| f.peer == c.addr(d) && f.counted),
        "not counted before it is added"
    );
    let (m, _) = client.add_member(&c.addr(d), false).await.unwrap();
    assert_eq!(m.version, v1 + 1);
    assert!(m.addrs.contains(&c.addr(d)));
    wait_committed(&c, l).await;
    c.wait_members(v1 + 1, SOON).await;
    c.wait_following(d, l, SOON).await;
    assert_eq!(c.members(d).addrs.len(), 4);
    assert!(members_file(&c, d).contains(&c.addr(d)));
    c.wait("the new node counted", SOON, || {
        c.followers(l)
            .iter()
            .any(|f| f.peer == c.addr(d) && f.counted)
    })
    .await;
    // Four nodes: a majority is three. With the two old followers held,
    // the leader and the new node make only two: the commit pends; the
    // new node alone cannot confirm... unless one old follower is free.
    c.hold_apply(others[0], true);
    c.hold_apply(others[1], true);
    let pending =
        tokio::time::timeout(Duration::from_millis(800), client.commit(free("held"))).await;
    assert!(pending.is_err(), "two of four cannot confirm: {pending:?}");
    c.hold_apply(others[1], false);
    let ok = c
        .client_patient(l)
        .await
        .commit(free("three of four"))
        .await
        .unwrap();
    assert!(ok.first >= 1);
    c.hold_apply(others[0], false);
    let head = c.node(l).default_db().db.head();
    c.wait_heads(head, SOON).await;
    let m = client.members().await.unwrap();
    assert!(m.committed && m.member);
    assert_eq!(m.members.addrs.len(), 4);
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_follower_drops_the_quorum_to_two_of_three() {
    let mut c = Cluster::start(4, TTL).await;
    let (l, others) = settled(&c).await;
    let gone = others[0];
    let client = c.client(l).await;
    let (m, _) = client.remove_member(&c.addr(gone)).await.unwrap();
    assert_eq!(m.addrs.len(), 3);
    wait_committed(&c, l).await;
    c.wait_removed(gone, SOON).await;
    // Three members: a majority is two. Both other followers held, the
    // removed one applying: it does not count, so the commit pends.
    c.hold_apply(others[1], true);
    c.hold_apply(others[2], true);
    let pending =
        tokio::time::timeout(Duration::from_millis(800), client.commit(free("held"))).await;
    assert!(
        pending.is_err(),
        "a removed node does not count: {pending:?}"
    );
    c.hold_apply(others[2], false);
    c.client_patient(l)
        .await
        .commit(free("two of three"))
        .await
        .unwrap();
    c.hold_apply(others[1], false);
    // The removed node keeps reading and refuses writes.
    let r = c.client(gone).await.commit(free("nope")).await;
    match r {
        Err(ClientError::Fenced { message, .. }) => {
            assert!(message.contains("not a member"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    let head = c.node(l).default_db().db.head();
    c.wait_heads(head, SOON).await;
    assert_eq!(
        c.client(gone).await.info().await.unwrap().role,
        WireRole::Removed
    );
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_the_leader_elects_another_and_the_old_one_is_removed() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, _) = settled(&c).await;
    let client = c.client(l).await;
    let before = client.commit(free("before")).await.unwrap().first;
    let (m, _) = client.remove_member(&c.addr(l)).await.unwrap();
    assert!(!m.addrs.contains(&c.addr(l)));
    c.wait("the old leader removed", SOON, || {
        c.role(l) == Some(Role::Removed)
    })
    .await;
    let r = client.commit(free("after")).await;
    assert!(matches!(r, Err(ClientError::Fenced { .. })), "{r:?}");
    let l2 = c.wait_for_leader(3 * SOON).await;
    assert_ne!(l2, l);
    assert_eq!(c.members(l2).addrs.len(), 2);
    let after = c
        .client(l2)
        .await
        .commit(free("after"))
        .await
        .unwrap()
        .first;
    assert!(after > before);
    // The removed node follows the new leader for reads.
    c.wait("the removed node following", SOON, || {
        c.node(l).leader_addr().as_deref() == Some(&c.addr(l2))
            && c.node(l).default_db().db.head() == after + 1
    })
    .await;
    assert_eq!(
        c.client(l)
            .await
            .read_all_with(0, 0, chrond::testing::local_read())
            .await
            .unwrap()
            .len() as u64,
        after + 1
    );
    let st = c.client(l).await.members().await.unwrap();
    assert!(!st.member);
    assert_eq!(st.leader_addr.as_deref(), Some(c.addr(l2).as_str()));
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_change_is_refused_while_one_is_pending() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let d = c.add_node(l).await;
    wait_removed_following(&c, d, l).await;
    // One old follower and the new node held before a write: the write
    // is confirmed by the other two, and the new majority (three of
    // four) cannot hold it, so the change stays pending.
    c.hold_apply(d, true);
    c.hold_apply(others[0], true);
    client.commit(free("w")).await.unwrap();
    let (_, committed) = client.add_member(&c.addr(d), false).await.unwrap();
    assert!(!committed, "pending while the new majority lacks the head");
    assert!(!c.members_committed(l));
    let r = client.remove_member(&c.addr(others[1])).await;
    match r {
        Err(ClientError::FailedPrecondition(m)) => assert!(m.contains("pending"), "{m}"),
        other => panic!("{other:?}"),
    }
    c.hold_apply(d, false);
    wait_committed(&c, l).await;
    c.hold_apply(others[0], false);
    let (m, _) = client.remove_member(&c.addr(others[1])).await.unwrap();
    assert_eq!(m.addrs.len(), 3);
    wait_committed(&c, l).await;
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_change_survives_restarts_and_the_flags_are_ignored() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, _) = settled(&c).await;
    let d = c.add_node(l).await;
    wait_removed_following(&c, d, l).await;
    let (m, _) = c
        .client(l)
        .await
        .add_member(&c.addr(d), false)
        .await
        .unwrap();
    wait_committed(&c, l).await;
    c.wait_members(m.version, SOON).await;
    let three: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    for i in 0..3 {
        c.stop(i).await;
        let peers: Vec<String> = three.iter().filter(|a| **a != c.addr(i)).cloned().collect();
        c.restart_with(i, move |o| o.peers = peers).await;
        assert_eq!(c.members(i).version, m.version, "node {i} reads the file");
        assert!(c.members(i).addrs.contains(&c.addr(d)));
        c.wait_for_leader(3 * SOON).await;
    }
    // A grouped directory refuses to start without --peer.
    c.stop(d).await;
    let r = c.try_restart_with(d, |o| o.peers = vec![]).await;
    let err = r.expect_err("a grouped directory needs --peer");
    assert!(
        err.to_string().contains("belongs to a failover group"),
        "{err}"
    );
    c.stop_all().await;
}

/// A single node with one (fake) peer: raw heartbeats and votes probe
/// the membership rules without a second daemon.
async fn one_with_a_fake_peer() -> (Cluster, Client, String) {
    let mut c = Cluster::start(1, TTL).await;
    c.stop(0).await;
    c.restart_with(0, |o| o.peers = vec!["http://127.0.0.1:1".into()])
        .await;
    let client = c.client(0).await;
    (c, client, "http://127.0.0.1:1".to_string())
}

fn members_wire(epoch: u64, version: u64, addrs: &[&str]) -> Option<pb::Members> {
    Some(pb::Members {
        epoch,
        version,
        addrs: addrs.iter().map(|s| s.to_string()).collect(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_node_learns_the_membership_from_a_heartbeat_and_refuses_an_older_leader() {
    let (mut c, client, peer) = one_with_a_fake_peer().await;
    let me = c.addr(0);
    assert_eq!(c.members(0).generation(), (0, 1));
    let hb = |epoch: u64, members: Option<pb::Members>| pb::HeartbeatRequest {
        epoch,
        leader_addr: peer.clone(),
        head: 0,
        lease_ttl_ms: 1000,
        members,
        confirmed: Vec::new(),
    };
    // A higher generation is persisted before the acknowledgment.
    let r = client
        .heartbeat(hb(1, members_wire(0, 2, &[&me, &peer, "http://d:1"])))
        .await
        .unwrap();
    assert!(r.ok, "{r:?}");
    assert_eq!(c.members(0).generation(), (0, 2));
    assert!(
        members_file(&c, 0).contains("http://d:1"),
        "{}",
        members_file(&c, 0)
    );
    // A lower one is refused, and the answer carries this node's.
    let r = client
        .heartbeat(hb(1, members_wire(0, 1, &[&me, &peer])))
        .await
        .unwrap();
    assert!(!r.ok);
    assert_eq!(r.members.map(|m| m.version), Some(2));
    assert_eq!(c.members(0).generation(), (0, 2), "unchanged");
    // Generations order by epoch first: (1, 1) outranks (0, 2).
    let r = client
        .heartbeat(hb(1, members_wire(1, 1, &[&me, &peer])))
        .await
        .unwrap();
    assert!(r.ok, "{r:?}");
    assert_eq!(c.members(0).generation(), (1, 1));
    assert!(!members_file(&c, 0).contains("http://d:1"));
    // A vote request behind on the membership is refused with it.
    let r = client
        .request_vote(pb::RequestVoteRequest {
            epoch: 5,
            candidate_addr: peer.clone(),
            last_epoch: 5,
            databases: vec![],
            forced: true,
            pre_vote: true,
            members: members_wire(0, 9, &[&me, &peer]),
        })
        .await
        .unwrap();
    assert!(!r.granted && r.reason.contains("membership"), "{r:?}");
    assert_eq!(r.members.map(|m| (m.epoch, m.version)), Some((1, 1)));
    // A heartbeat without a membership (an older daemon) is taken as is.
    let r = client.heartbeat(hb(1, None)).await.unwrap();
    assert!(r.ok);
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_member_cannot_heartbeat_or_be_voted_for() {
    let (mut c, client, peer) = one_with_a_fake_peer().await;
    let vote = |who: &str| pb::RequestVoteRequest {
        epoch: 5,
        candidate_addr: who.to_string(),
        last_epoch: 5,
        databases: vec![],
        forced: true,
        pre_vote: false,
        members: None,
    };
    let r = client.request_vote(vote("http://z:1")).await.unwrap();
    assert!(!r.granted && r.reason.contains("not a member"), "{r:?}");
    assert!(
        !c.nodes[0].dir.path().join("data").join("vote").exists(),
        "no vote recorded"
    );
    let hb = |who: &str| pb::HeartbeatRequest {
        epoch: 6,
        leader_addr: who.to_string(),
        head: 0,
        lease_ttl_ms: 1000,
        members: None,
        confirmed: Vec::new(),
    };
    let r = client.heartbeat(hb("http://z:1")).await.unwrap();
    assert!(!r.ok, "{r:?}");
    assert_eq!(c.role(0), Some(Role::Candidate), "not following a stranger");
    assert_eq!(c.node(0).leader_addr(), None);
    // The control: without the check both go through, as before 0.5.0.
    c.node(0).skip_member_check.store(true, Ordering::Relaxed);
    let r = client.request_vote(vote("http://z:1")).await.unwrap();
    assert!(r.granted, "{r:?}");
    let r = client.heartbeat(hb("http://z:1")).await.unwrap();
    assert!(r.ok, "{r:?}");
    assert_eq!(c.node(0).leader_addr().as_deref(), Some("http://z:1"));
    // The configured peer is a member and may do both.
    c.node(0).skip_member_check.store(false, Ordering::Relaxed);
    let r = client
        .heartbeat(pb::HeartbeatRequest {
            epoch: 7,
            ..hb(&peer)
        })
        .await
        .unwrap();
    assert!(r.ok, "{r:?}");
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_leader_republishes_the_membership_under_its_epoch() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, _) = settled(&c).await;
    let m = c.members(l);
    assert_eq!(
        m.epoch,
        c.node(l).data_epoch(),
        "stamped with the leader's epoch"
    );
    assert_eq!(m.version, 1, "the version does not move on an election");
    c.wait_members(1, SOON).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(3 * SOON).await;
    let m2 = c.members(l2);
    assert_eq!(m2.epoch, c.node(l2).data_epoch());
    assert!(m2.epoch > m.epoch);
    assert_eq!(m2.version, 1);
    assert_eq!(m2.addrs, m.addrs);
    wait_committed(&c, l2).await;
    c.stop_all().await;
    // The control: without the republish a new leader keeps the old
    // epoch on its membership, so an older leader's later generation
    // would outrank it.
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.node(*i).skip_restamp.store(true, Ordering::Relaxed);
    }
    let m = c.members(l);
    c.stop(l).await;
    let l2 = c.wait_for_leader(3 * SOON).await;
    assert_eq!(c.members(l2).epoch, m.epoch, "the control: not restamped");
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_write_survives_leader_death_right_after_a_change() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let (a, b) = (others[0], others[1]);
    c.hold_apply(b, true);
    let d = c.add_node(l).await;
    wait_removed_following(&c, d, l).await;
    let client = c.client(l).await;
    let w = client.commit(free("w")).await.unwrap();
    client.add_member(&c.addr(d), false).await.unwrap();
    wait_committed(&c, l).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(3 * SOON).await;
    assert!(l2 == a || l2 == d, "{}", c.describe());
    let log = c.client(l2).await.read_all(0, 0).await.unwrap();
    assert_eq!(log[w.first as usize].payload, "w");
    c.hold_apply(b, false);
    c.stop_all().await;
}

/// The second commit condition: a change commits only once a majority
/// of the new membership holds every write confirmed before it. Without
/// it, adding two lagging nodes lets them elect a leader without the
/// write.
#[tokio::test(flavor = "multi_thread")]
async fn a_change_waits_until_the_new_majority_holds_every_confirmed_write() {
    for control in [false, true] {
        let mut c = Cluster::start(3, TTL).await;
        let (a, others) = settled(&c).await;
        let (b, cc) = (others[0], others[1]);
        if control {
            c.node(a).skip_catchup_rule.store(true, Ordering::Relaxed);
        }
        c.hold_apply(cc, true);
        let client = c.client(a).await;
        let w = client.commit(free("w")).await.unwrap();
        assert_eq!(w.first, 0, "confirmed by a and b");
        // d and e join and are held: they acknowledge heartbeats but
        // hold nothing of w.
        let d = c.add_node(a).await;
        c.hold_apply(d, true);
        wait_removed_following(&c, d, a).await;
        assert_eq!(
            c.node(d).default_db().db.head(),
            0,
            "held before it attached"
        );
        let (_, committed) = client.add_member(&c.addr(d), false).await.unwrap();
        if control {
            wait_committed(&c, a).await;
        } else {
            assert!(!committed, "d holds nothing of w yet");
            assert!(!c.members_committed(a));
        }
        let e = c.add_node(a).await;
        c.hold_apply(e, true);
        wait_removed_following(&c, e, a).await;
        assert_eq!(
            c.node(e).default_db().db.head(),
            0,
            "held before it attached"
        );
        let r = client.add_member(&c.addr(e), false).await;
        if control {
            r.unwrap();
            wait_committed(&c, a).await;
        } else {
            assert!(
                matches!(r, Err(ClientError::FailedPrecondition(_))),
                "pending: {r:?}"
            );
        }
        // a and b die; c, d, e remain held: none applied w, and a held
        // node elected stops its puller, so the chunk it received goes.
        c.stop(a).await;
        c.stop(b).await;
        if control {
            let l = c.wait_for_leader(3 * SOON).await;
            let log = c.client(l).await.read_all(0, 0).await.unwrap();
            assert!(
                log.first().is_none_or(|e| e.payload != "w"),
                "the control: w is lost on {l}: {}",
                c.describe()
            );
        } else {
            // The membership is still {a,b,c,d} (pending or not): three
            // are needed, c and d are two; e was never in it.
            tokio::time::sleep(3 * TTL).await;
            assert_eq!(
                c.leader(),
                None,
                "no majority without a or b: {}",
                c.describe()
            );
            c.restart(b).await;
            let l = c.wait_for_leader(3 * SOON).await;
            let log = c
                .client(l)
                .await
                .read_all_with(0, 0, chrond::testing::local_read())
                .await
                .unwrap();
            assert_eq!(log[0].payload, "w", "{}", c.describe());
        }
        for i in [cc, d, e] {
            c.hold_apply(i, false);
        }
        c.stop_all().await;
    }
}

fn append_probe(
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
            match c.commit(free(&format!("p{i}-{n}"))).await {
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
async fn at_most_one_node_accepts_writes_across_adds_and_removes_under_kills() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, _) = settled(&c).await;
    let stop = Arc::new(AtomicBool::new(false));
    let successes = Arc::new(Mutex::new(Vec::new()));
    let mut probes: Vec<_> = (0..3)
        .map(|i| append_probe(c.addr(i), i, stop.clone(), successes.clone()))
        .collect();
    let d = c.add_node(l).await;
    probes.push(append_probe(c.addr(d), d, stop.clone(), successes.clone()));
    wait_removed_following(&c, d, l).await;
    c.client(l)
        .await
        .add_member(&c.addr(d), false)
        .await
        .unwrap();
    wait_committed(&c, l).await;
    tokio::time::sleep(TTL).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(3 * SOON).await;
    assert_ne!(l2, l);
    // The new leader's own republished membership commits first; a
    // change asked before that is answered "pending", as an operator
    // would see it.
    wait_committed(&c, l2).await;
    // The dead node is removed, restarted (removed), and added again.
    c.client(l2).await.remove_member(&c.addr(l)).await.unwrap();
    wait_committed(&c, l2).await;
    c.restart(l).await;
    wait_removed_following(&c, l, l2).await;
    c.client(l2)
        .await
        .add_member(&c.addr(l), false)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{}", c.describe()));
    wait_committed(&c, l2).await;
    c.wait_following(l, l2, SOON).await;
    tokio::time::sleep(TTL).await;
    c.partition(l2);
    let l3 = {
        let mut found = None;
        c.wait("another leader", 3 * SOON, || {
            found = c.leader().filter(|x| *x != l2);
            found.is_some()
        })
        .await;
        found.unwrap()
    };
    tokio::time::sleep(TTL).await;
    c.heal(l2);
    c.wait_following(l2, l3, SOON).await;
    tokio::time::sleep(TTL).await;
    stop.store(true, Ordering::Relaxed);
    for p in probes {
        let _ = tokio::time::timeout(Duration::from_secs(5), p).await;
    }
    let all = successes.lock().unwrap().clone();
    let writers: std::collections::BTreeSet<usize> = all.iter().map(|s| s.0).collect();
    assert!(writers.len() >= 2, "leadership never moved: {writers:?}");
    let mut overlaps = 0;
    for x in &all {
        for y in &all {
            if x.0 < y.0 && x.1 < y.2 && y.1 < x.2 {
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
