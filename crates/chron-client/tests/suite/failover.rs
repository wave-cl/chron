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
async fn a_commit_follows_a_failover_and_reads_do_not() {
    let mut c = Cluster::start(3, TTL).await;
    let l = c.wait_for_leader(SOON).await;
    for i in 0..3 {
        if i != l {
            c.wait_following(i, l, SOON).await;
        }
    }
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    let client = Client::connect_any(seeds, config()).await.unwrap();
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
