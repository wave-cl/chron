//! `connect_any` finds the leader from any seed and a commit follows a
//! failover.

use std::time::Duration;

use chron_client::{Client, ClientConfig, ClientError, NewEvent, Role, Transaction};
use chrond::testing::Cluster;

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);

fn config() -> ClientConfig {
    ClientConfig {
        connect_timeout: Duration::from_millis(500),
        request_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    }
}

/// The 0.4.0 rule: no commit ids, so a resend is only safe where a
/// second landing would conflict.
fn config_without_ids() -> ClientConfig {
    ClientConfig {
        commit_ids: false,
        ..config()
    }
}

async fn free(client: &Client, payload: &str) -> Result<u64, ClientError> {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    client.commit(t).await.map(|c| c.first)
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_any_finds_the_leader_from_any_seed() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    for seed in 0..3 {
        let client = Client::connect_any([c.addr(seed)], config()).await.unwrap();
        assert_eq!(client.addr(), c.addr(l), "seed {seed}");
        assert_eq!(client.info().await.unwrap().role, Role::Leader);
        free(&client, "via any seed").await.unwrap();
    }
    // A dead seed first, then a live one.
    let client = Client::connect_any(
        ["http://127.0.0.1:1".to_string(), c.addr((l + 1) % 3)],
        config(),
    )
    .await
    .unwrap();
    assert_eq!(client.addr(), c.addr(l));
    assert_eq!(client.seeds().len(), 2);
    // A database binding shares the connection.
    let other = client.database("default");
    assert_eq!(other.addr(), c.addr(l));
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_ids_a_commit_follows_a_failover_only_where_a_second_landing_conflicts() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    let client = Client::connect_any(seeds, config_without_ids())
        .await
        .unwrap();
    let bound = client.database("default");
    let first = free(&client, "before").await.unwrap();
    c.wait_heads(first + 1, SOON).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    // The old connection is dead. A free append may have landed before
    // the connection broke, so it is not resent: the caller decides.
    let r = free(&client, "after").await;
    assert!(
        matches!(
            r,
            Err(ClientError::Unavailable(_) | ClientError::Transport(_))
        ),
        "{r:?}"
    );
    // An append with an exact expectation is resent to the leader found
    // next, since a second landing would be a conflict.
    let id = chron_client::Uuid::from_u128(9);
    let p = client
        .append(
            id,
            chron_client::Expected::Exact(0),
            vec![NewEvent::new("after")],
        )
        .await
        .unwrap()
        .first;
    assert_eq!(p, first + 1);
    assert_eq!(client.addr(), c.addr(l2));
    assert_eq!(bound.addr(), c.addr(l2), "the binding followed the swap");
    assert_eq!(client.head().await.unwrap(), first + 2);
    // A commit refused by a node that no longer leads follows too.
    c.restart(l).await;
    c.wait_following(l, l2, SOON).await;
    let stale = Client::connect(c.addr(l), config()).await.unwrap();
    let r = free(&stale, "stale").await;
    assert!(matches!(r, Err(ClientError::Fenced { .. })), "{r:?}");
    assert!(
        stale.rediscover().await.is_err(),
        "not a connect_any client"
    );
    c.stop_all().await;
}

/// With commit ids (the default) every commit follows a failover: a
/// free append whose connection broke is resent and lands exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn a_free_append_is_resent_with_an_id_and_lands_once() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    c.wait("followers attached", SOON, || c.followers(l).len() == 2)
        .await;
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    let client = Client::connect_any(seeds, config()).await.unwrap();
    let first = free(&client, "before").await.unwrap();
    c.wait_heads(first + 1, SOON).await;
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let p = free(&client, "after").await.unwrap();
    assert_eq!(p, first + 1);
    assert_eq!(client.addr(), c.addr(l2));
    let log = client.read_all(0, 0).await.unwrap();
    assert_eq!(
        log.iter().filter(|e| e.payload == "after").count(),
        1,
        "landed once"
    );
    assert!(
        log[(first + 1) as usize].commit_id.is_some(),
        "the client chose an id"
    );
    // An epoch token travels with the resend; a fenced answer at an
    // epoch above the token is final rather than retried for 30 s.
    let stale_epoch = client.info().await.unwrap().epoch - 1;
    let r = client
        .commit_with(
            {
                let mut t = Transaction::new();
                t.append_free(vec![NewEvent::new("stale")]);
                t
            },
            chron_client::CommitOptions {
                epoch: Some(stale_epoch),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(r, Err(ClientError::Fenced { .. })), "{r:?}");
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_leader_is_an_error_that_names_every_seed() {
    let mut c = Cluster::start(2, TTL).await;
    // Two nodes cannot elect without each other: cut them apart.
    c.cut(0, 1);
    tokio::time::sleep(2 * TTL).await;
    let seeds: Vec<String> = vec![c.addr(0), c.addr(1), "http://127.0.0.1:1".into()];
    let r = Client::connect_any(seeds, config()).await;
    match r {
        Err(ClientError::NoLeader(m)) => {
            assert!(
                m.contains("knows no leader") && m.contains("127.0.0.1:1"),
                "{m}"
            );
        }
        other => panic!("{other:?}"),
    }
    let r = Client::connect_any(Vec::<String>::new(), config()).await;
    assert!(matches!(r, Err(ClientError::InvalidArgument(_))), "{r:?}");
    c.uncut(0, 1);
    c.wait_for_leader(SOON).await;
    let client = Client::connect_any([c.addr(1)], config()).await.unwrap();
    free(&client, "healed").await.unwrap();
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unconfirmed_commit_is_retried_with_an_id_and_ends_once() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(300))
    })
    .await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    c.wait("followers attached", SOON, || c.followers(l).len() == 2)
        .await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    let client = Client::connect_any(
        seeds,
        ClientConfig {
            failover_timeout: Duration::from_secs(20),
            ..config()
        },
    )
    .await
    .unwrap();
    let id = chron_client::Uuid::from_u128(3);
    let head_before = c.node(l).default_db().db.head();
    let pending = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .append(
                    id,
                    chron_client::Expected::Exact(0),
                    vec![NewEvent::new("once")],
                )
                .await
        }
    });
    // Unconfirmed on the leader, the client keeps resending it; releasing
    // the followers lets a resend be confirmed as a duplicate.
    c.wait("the first attempt landed", SOON, || {
        c.node(l).default_db().db.head() == head_before + 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!pending.is_finished(), "still retrying");
    for i in &others {
        c.hold_apply(*i, false);
    }
    let c1 = tokio::time::timeout(SOON, pending)
        .await
        .expect("ends once the followers apply")
        .unwrap()
        .unwrap();
    assert_eq!(
        (c1.first, c1.next, c1.duplicate),
        (head_before, head_before + 1, true)
    );
    assert_eq!(
        c.node(l).default_db().db.head(),
        head_before + 1,
        "landed once"
    );
    assert_eq!(client.aggregate_len(id).await.unwrap(), 1);
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_ids_off_keeps_the_old_rule_for_an_unconfirmed_commit() {
    let mut c = Cluster::start_with(3, TTL, |o| {
        o.sync_timeout = Some(Duration::from_millis(300))
    })
    .await;
    let l = c.wait_for_leader(SOON).await;
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    for i in &others {
        c.wait_following(*i, l, SOON).await;
    }
    c.wait("followers attached", SOON, || c.followers(l).len() == 2)
        .await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    let client = Client::connect_any(seeds, config_without_ids())
        .await
        .unwrap();
    let id = chron_client::Uuid::from_u128(3);
    let head_before = c.node(l).default_db().db.head();
    let r = client
        .append(
            id,
            chron_client::Expected::Exact(0),
            vec![NewEvent::new("once")],
        )
        .await;
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
    assert_eq!(
        c.node(l).default_db().db.head(),
        head_before + 1,
        "landed once, not retried"
    );
    let r = client
        .commit_with(
            free_txn("async"),
            chron_client::CommitOptions {
                epoch: None,
                sync: Some(false),
                commit_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(r.first, head_before + 1);
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.wait_heads(head_before + 2, SOON).await;
    c.stop_all().await;
}

fn free_txn(payload: &str) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    t
}
