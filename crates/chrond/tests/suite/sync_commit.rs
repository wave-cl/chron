//! Synchronous replication: a commit on a failover group is acknowledged
//! only once a majority durably holds it.

use std::time::Duration;

use chron_client::{Client, ClientError, CommitOptions, Expected, NewEvent, Transaction, Uuid};
use chrond::SyncCommit;
use chrond::node::Role;
use chrond::testing::Cluster;

use crate::common::Server;

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);

fn free(payload: &str) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    t
}

async fn settled(c: &Cluster) -> (usize, Vec<usize>) {
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..c.nodes.len()).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    // Followers are attached once the leader lists them, and have taken
    // its epoch once they applied its opening chunk.
    c.wait("every follower attached", SOON, || {
        c.followers(l).len() == others.len()
    })
    .await;
    let epoch = c.node(l).data_epoch();
    c.wait("every follower at the leader's epoch", SOON, || {
        others.iter().all(|i| c.node(*i).data_epoch() == epoch)
    })
    .await;
    (l, others)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_commit_is_acknowledged_only_after_a_follower_applied_it() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let client = c.client_patient(l).await;
    let head_before = c.node(l).default_db().db.head();
    let mut pending = Box::pin(client.commit(free("held")));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut pending)
            .await
            .is_err(),
        "the commit was acknowledged with every follower held"
    );
    assert_eq!(
        c.node(l).default_db().db.head(),
        head_before + 1,
        "the write is on the leader's disk meanwhile"
    );
    c.hold_apply(others[0], false);
    let committed = pending.await.unwrap();
    assert_eq!(committed.next, head_before + 1);
    assert_eq!(c.majority_applied(l), committed.next);
    let f = c.followers(l);
    assert!(
        f.iter().any(|f| f.applied >= committed.next && f.counted),
        "{f:?}"
    );
    assert!(c.node(l).metrics_sync_waits() >= 1);
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_commit_times_out_as_unconfirmed_and_still_replicates() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(300))
    })
    .await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let client = c.client(l).await;
    let id = Uuid::from_u128(5);
    let r = client
        .append(id, Expected::Exact(0), vec![NewEvent::new("a")])
        .await;
    let (first, next) = match r {
        Err(ClientError::Unconfirmed { first, next, .. }) => (first, next),
        other => panic!("{other:?}"),
    };
    assert_eq!((first, next), (0, 1));
    assert_eq!(c.node(l).default_db().db.head(), 1);
    assert_eq!(c.node(l).metrics_sync_timeouts("timeout"), 1);
    // A blind resend conflicts: the write is there.
    let r = client
        .append(id, Expected::Exact(0), vec![NewEvent::new("a")])
        .await;
    assert!(
        matches!(r, Err(ClientError::Conflict { actual: 1, .. })),
        "{r:?}"
    );
    // Without the wait the commit returns at once.
    let c2 = client
        .commit_with(
            free("async"),
            CommitOptions {
                epoch: None,
                sync: Some(false),
                commit_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(c2.first, 1);
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.wait_heads(2, SOON).await;
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn waiters_fail_fast_when_the_leader_stops_leading() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let client = c.client_patient(l).await;
    let mut pending = Box::pin(client.commit(free("fenced-while-waiting")));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    let epoch = c.node(l).data_epoch();
    c.client(l).await.fence(epoch + 1).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(500), pending)
        .await
        .expect("the waiter ended with the role");
    assert!(matches!(r, Err(ClientError::Unconfirmed { .. })), "{r:?}");
    assert_eq!(c.node(l).metrics_sync_timeouts("role_change"), 1);
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_per_request_choice_wins_over_the_daemon_default() {
    let mut c = Cluster::start_with(3, TTL, |o| o.sync_commit = SyncCommit::Off).await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let client = c.client_patient(l).await;
    client.commit(free("off by default")).await.unwrap();
    assert!(!client.info().await.unwrap().sync_commit);
    let mut pending = Box::pin(client.commit_with(
        free("sync on request"),
        CommitOptions {
            epoch: None,
            sync: Some(true),
            commit_id: None,
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut pending)
            .await
            .is_err()
    );
    for i in &others {
        c.hold_apply(*i, false);
    }
    pending.await.unwrap();
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_peers_treats_sync_as_local() {
    let s = Server::start().await;
    let client = Client::connect(s.addr(), Default::default()).await.unwrap();
    client
        .commit_with(
            free("lone"),
            CommitOptions {
                epoch: None,
                sync: Some(true),
                commit_id: None,
            },
        )
        .await
        .unwrap();
    let info = client.info().await.unwrap();
    assert!(!info.sync_commit);
    assert_eq!(info.majority_applied, None);
    assert!(info.followers.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_without_fsync_does_not_count_for_a_leader_with_fsync() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(500))
    })
    .await;
    let (l, others) = settled(&c).await;
    // The leader restarts with fsync and takes the lead back.
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    c.restart_with(l, |o| {
        o.fsync = true;
        o.sync_timeout = Some(Duration::from_millis(500));
    })
    .await;
    c.wait_following(l, l2, SOON).await;
    c.client(l).await.promote().await.unwrap();
    c.wait("the durable node leading", SOON, || c.leader() == Some(l))
        .await;
    c.wait("both followers attached", SOON, || {
        c.followers(l).len() == 2
    })
    .await;
    let f = c.followers(l);
    assert!(f.iter().all(|f| !f.durable && !f.counted), "{f:?}");
    let client = c.client(l).await;
    let r = client.commit(free("nobody durable")).await;
    assert!(matches!(r, Err(ClientError::Unconfirmed { .. })), "{r:?}");
    // One durable follower makes a majority of two.
    let f1 = others[0];
    c.stop(f1).await;
    c.restart_with(f1, |o| {
        o.fsync = true;
        o.sync_timeout = Some(Duration::from_millis(500));
    })
    .await;
    c.wait_following(f1, l, SOON).await;
    c.wait("the durable follower counted", SOON, || {
        c.followers(l).iter().any(|f| f.counted)
    })
    .await;
    client.commit(free("one durable")).await.unwrap();
    c.stop_all().await;
}

/// A write a majority confirmed survives two leader deaths in a row: a
/// follower that never applied what the second leader held cannot claim
/// that leader's epoch, so the restarted first leader, which holds the
/// write, is the one elected.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_write_survives_two_leader_deaths() {
    let mut c = Cluster::start(3, TTL).await;
    let (l1, others) = settled(&c).await;
    let (a, b) = (others[0], others[1]);
    c.hold_apply(b, true);
    let w = c
        .client(l1)
        .await
        .commit(free("confirmed by a"))
        .await
        .unwrap();
    assert_eq!(w.first, 0);
    let epoch = c.node(l1).data_epoch();
    c.stop(l1).await;
    c.wait("a leading", SOON, || c.leader() == Some(a)).await;
    tokio::time::sleep(TTL).await;
    assert_eq!(
        c.node(b).data_epoch(),
        epoch,
        "b applied nothing of a's, so it stays at its epoch"
    );
    c.stop(a).await;
    c.restart(l1).await;
    let leader = c.wait_for_leader(3 * SOON).await;
    assert_eq!(leader, l1, "{}", c.describe());
    let log = c.client(l1).await.read_all(0, 0).await.unwrap();
    assert_eq!(log[0].payload, "confirmed by a");
    c.hold_apply(b, false);
    c.wait_following(b, l1, SOON).await;
    c.wait_heads(1, SOON).await;
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_without_follow_gets_no_acknowledgments() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(500))
    })
    .await;
    let (l, others) = settled(&c).await;
    c.node(l)
        .refuse_follow
        .store(true, std::sync::atomic::Ordering::Relaxed);
    for i in &others {
        c.stop(*i).await;
    }
    for i in &others {
        c.restart_with(*i, |o| o.sync_timeout = Some(Duration::from_millis(500)))
            .await;
    }
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    let client = c.client(l).await;
    let head = client
        .commit_with(
            free("plain"),
            CommitOptions {
                epoch: None,
                sync: Some(false),
                commit_id: None,
            },
        )
        .await
        .unwrap()
        .next;
    c.wait_heads(head, SOON).await;
    assert!(c.followers(l).is_empty(), "{:?}", c.followers(l));
    let r = client.commit(free("sync")).await;
    assert!(matches!(r, Err(ClientError::Unconfirmed { .. })), "{r:?}");
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_created_later_is_followed_with_acknowledgments() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, _) = settled(&c).await;
    let client = c.client(l).await;
    client.create_database("orders").await.unwrap();
    let orders = client.database("orders");
    c.wait("the new database attached", SOON, || {
        c.node(l)
            .get("orders")
            .is_some_and(|d| d.sync.snapshot(&c.node(l).members()).len() == 2)
    })
    .await;
    let committed = orders.commit(free("first order")).await.unwrap();
    assert_eq!(committed.first, 0);
    assert!(c.role(l) == Some(Role::Leader));
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gateway_takes_sync_and_answers_504_when_unconfirmed() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(300));
        o.http_listen = Some("127.0.0.1:0".parse().unwrap());
    })
    .await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let addr = c.nodes[l].running.as_ref().unwrap().http_addr.unwrap();
    let body =
        serde_json::json!({"appends": [{"aggregate": "free", "events": [{"payload": "x"}]}]})
            .to_string();
    let (code, v, _) = crate::http::call(
        addr,
        "POST",
        "/v1/db/default/commit?sync=true".into(),
        vec![],
        Some(body.clone()),
    )
    .await;
    assert_eq!(code, 504, "{v}");
    assert_eq!(v["code"], "DeadlineExceeded");
    assert_eq!(
        (v["committed_first"].as_u64(), v["committed_next"].as_u64()),
        (Some(0), Some(1))
    );
    let (code, _, _) = crate::http::call(
        addr,
        "POST",
        "/v1/db/default/commit?sync=false".into(),
        vec![],
        Some(body.clone()),
    )
    .await;
    assert_eq!(code, 200);
    let (code, _, _) = crate::http::call(
        addr,
        "POST",
        "/v1/db/default/commit?sync=maybe".into(),
        vec![],
        Some(body),
    )
    .await;
    assert_eq!(code, 400);
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unconfirmed_commit_resent_with_its_id_is_a_duplicate() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(300))
    })
    .await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    // A single-address client: it never resends on its own.
    let client = c.client(l).await;
    let id = Uuid::from_u128(5);
    let options = CommitOptions {
        commit_id: Some([8; 16]),
        ..CommitOptions::default()
    };
    let txn = || {
        let mut t = Transaction::new();
        t.append(id, Expected::Exact(0), vec![NewEvent::new("once")]);
        t
    };
    let r = client.commit_with(txn(), options).await;
    assert!(
        matches!(
            r,
            Err(ClientError::Unconfirmed {
                first: 0,
                next: 1,
                ..
            })
        ),
        "{r:?}"
    );
    // Resent without the wait: the landing is there, so a duplicate,
    // whatever the expectation says now.
    let again = client
        .commit_with(
            txn(),
            CommitOptions {
                sync: Some(false),
                ..options
            },
        )
        .await
        .unwrap();
    assert_eq!((again.first, again.next, again.duplicate), (0, 1, true));
    assert_eq!(c.node(l).default_db().db.head(), 1);
    for i in &others {
        c.hold_apply(*i, false);
    }
    // Resent with the wait once the followers apply: confirmed, still a
    // duplicate.
    let confirmed = client
        .commit_with(
            txn(),
            CommitOptions {
                sync: Some(true),
                ..options
            },
        )
        .await
        .unwrap();
    assert_eq!((confirmed.first, confirmed.duplicate), (0, true));
    c.wait_heads(1, SOON).await;
    assert_eq!(c.node(l).default_db().db.head(), 1, "landed once");
    c.stop_all().await;
}
