//! Confirmed reads: on a node with peers a read, head or subscription
//! sees, by default under sync commits, only positions a majority
//! durably holds; `local` sees everything the node holds; `at_least`
//! waits for a position.

use std::time::{Duration, Instant};

use chron_client::{ClientError, CommitOptions, Consistency, NewEvent, ReadOptions, Transaction};
use chrond::SyncCommit;
use chrond::testing::Cluster;
use tokio_stream::StreamExt;

use crate::common::Server;

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);
const QUIET: Duration = Duration::from_millis(300);

fn free(payload: &str) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    t
}

fn with(c: Consistency) -> ReadOptions {
    ReadOptions {
        consistency: Some(c),
        ..Default::default()
    }
}

fn local_commit() -> CommitOptions {
    CommitOptions {
        sync: Some(false),
        ..Default::default()
    }
}

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
    (l, others)
}

/// A write on the leader's disk that no majority holds is invisible to
/// a confirmed read (the default under sync commits) and visible to a
/// local one; `head` reports both bounds; once a follower applied it,
/// the confirmed read sees it.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_follower_keeps_an_unconfirmed_write_out_of_confirmed_reads() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let first = client.commit(free("seen")).await.unwrap();
    assert_eq!(
        client.info().await.unwrap().read_consistency,
        Consistency::Confirmed
    );
    for i in &others {
        c.hold_apply(*i, true);
    }
    let held = client
        .commit_with(free("held"), local_commit())
        .await
        .unwrap();
    let seen = client.read_all(0, 0).await.unwrap();
    assert_eq!(
        seen.len() as u64,
        first.next,
        "the default read is confirmed"
    );
    let local = client
        .read_all_with(0, 0, with(Consistency::Local))
        .await
        .unwrap();
    assert_eq!(
        local.len() as u64,
        held.next,
        "a local read sees the held write"
    );
    let h = client.head_with(ReadOptions::default()).await.unwrap();
    assert_eq!(
        (h.head, h.confirmed, h.local),
        (first.next, first.next, held.next)
    );
    assert_eq!(client.head().await.unwrap(), first.next);
    assert!(
        client
            .read_all_with(0, 0, with(Consistency::Confirmed))
            .await
            .unwrap()
            .len() as u64
            == first.next
    );
    c.hold_apply(others[0], false);
    c.wait("the held write confirmed", SOON, || {
        c.node(l).default_db().confirmed() >= held.next
    })
    .await;
    assert_eq!(client.read_all(0, 0).await.unwrap().len() as u64, held.next);
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

/// An aggregate's length follows the consistency too: a held write on
/// the leader counts under local, not under confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_length_follows_the_consistency() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let a = chron_client::Uuid::from_u128(7);
    for i in 0..3 {
        client
            .append(
                a,
                chron_client::Expected::Exact(i),
                vec![NewEvent::new(format!("e{i}"))],
            )
            .await
            .unwrap();
    }
    for i in &others {
        c.hold_apply(*i, true);
    }
    let mut t = Transaction::new();
    t.append(
        a,
        chron_client::Expected::Exact(3),
        vec![NewEvent::new("held".to_string())],
    );
    client.commit_with(t, local_commit()).await.unwrap();
    assert_eq!(client.aggregate_len(a).await.unwrap(), 3, "confirmed");
    assert_eq!(
        client
            .aggregate_len_with(a, with(Consistency::Local))
            .await
            .unwrap(),
        4,
        "local"
    );
    c.hold_apply(others[0], false);
    c.wait("the held write confirmed", SOON, || {
        c.node(l).default_db().confirmed() >= 4
    })
    .await;
    assert_eq!(client.aggregate_len(a).await.unwrap(), 4);
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

/// A confirmed subscription delivers a write only once a majority
/// holds it; a local one at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_subscription_delivers_only_after_release() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let mut confirmed = client.subscribe(0).await.unwrap();
    let mut local = client
        .subscribe_with(0, None, Consistency::Local)
        .await
        .unwrap();
    for i in &others {
        c.hold_apply(*i, true);
    }
    let held = client
        .commit_with(free("held"), local_commit())
        .await
        .unwrap();
    let ev = tokio::time::timeout(SOON, local.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.position, held.first);
    assert!(
        tokio::time::timeout(QUIET, confirmed.next()).await.is_err(),
        "the confirmed subscription delivered an unconfirmed write"
    );
    c.hold_apply(others[0], false);
    let ev = tokio::time::timeout(SOON, confirmed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.position, held.first);
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

/// `at_least` waits for the bound to reach the position and fails with
/// the bound once its timeout passes.
#[tokio::test(flavor = "multi_thread")]
async fn at_least_waits_then_times_out() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let held = client
        .commit_with(free("held"), local_commit())
        .await
        .unwrap();
    let err = client
        .read_all_with(
            0,
            0,
            ReadOptions {
                consistency: None,
                at_least: Some(held.next),
                wait_timeout: Some(QUIET),
            },
        )
        .await
        .unwrap_err();
    match err {
        ClientError::BoundTimeout { position, head, .. } => {
            assert!(
                position < held.next,
                "the bound {position} is below {}",
                held.next
            );
            assert_eq!(head, held.next);
        }
        other => panic!("not a bound timeout: {other:?}"),
    }
    let n = c.node(others[0]);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        n.hold_apply.send_replace(false);
    });
    let t0 = Instant::now();
    let got = client
        .read_all_with(
            0,
            0,
            ReadOptions {
                consistency: None,
                at_least: Some(held.next),
                wait_timeout: Some(SOON),
            },
        )
        .await
        .unwrap();
    assert_eq!(got.len() as u64, held.next);
    assert!(
        t0.elapsed() >= Duration::from_millis(150),
        "the read waited"
    );
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

/// A follower learns the leader's confirmed bound from the chunks (at
/// once, with the next write) and from heartbeats (a quiet group), and
/// serves confirmed reads from it.
#[tokio::test(flavor = "multi_thread")]
async fn a_replicas_confirmed_bound_follows_the_leaders_majority() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let w1 = client.commit(free("one")).await.unwrap();
    let w2 = client.commit(free("two")).await.unwrap();
    // The second chunk carried the first write's confirmation to the
    // follower whose report confirmed it, before the commit returned:
    // at least one follower knows it at once, well before a heartbeat
    // (a third of the ttl) could have told it.
    c.wait("the first write confirmed on a follower", QUIET, || {
        others
            .iter()
            .any(|i| c.node(*i).default_db().confirmed() >= w1.next)
    })
    .await;
    // The second write's confirmation arrives with a heartbeat.
    c.wait("the second write confirmed on every follower", SOON, || {
        others
            .iter()
            .all(|i| c.node(*i).default_db().confirmed() >= w2.next)
    })
    .await;
    let fc = c.client(others[0]).await;
    let h = fc.head_with(with(Consistency::Confirmed)).await.unwrap();
    assert_eq!((h.head, h.confirmed, h.local), (w2.next, w2.next, w2.next));
    assert_eq!(fc.read_all(0, 0).await.unwrap().len() as u64, w2.next);
    c.stop_all().await;
}

/// The bound a node learned as a follower survives its promotion: the
/// new leader's confirmed reads serve what was confirmed before.
#[tokio::test(flavor = "multi_thread")]
async fn confirmed_reads_recover_after_a_promotion() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let mut next = 0;
    for i in 0..5 {
        next = client.commit(free(&format!("w{i}"))).await.unwrap().next;
    }
    c.wait("the writes confirmed on every follower", SOON, || {
        others
            .iter()
            .all(|i| c.node(*i).default_db().confirmed() >= next)
    })
    .await;
    // Held followers report nothing to the new leader, so what it serves
    // as confirmed is what it learned before the election.
    for i in &others {
        c.hold_apply(*i, true);
    }
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    assert!(
        c.node(l2).default_db().confirmed() >= next,
        "the promotion kept the bound"
    );
    let c2 = c.client(l2).await;
    assert_eq!(c2.read_all(0, 0).await.unwrap().len() as u64, next);
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.stop_all().await;
}

/// A commit's `next` is the token: a read on a replica with `at_least`
/// returns the write once the replica holds it.
#[tokio::test(flavor = "multi_thread")]
async fn the_token_flow_reads_your_write_on_a_replica() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let w = c.client(l).await.commit(free("mine")).await.unwrap();
    let fc = c.client(others[0]).await;
    let got = fc
        .read_all_with(
            0,
            0,
            ReadOptions {
                consistency: None,
                at_least: Some(w.next),
                wait_timeout: Some(SOON),
            },
        )
        .await
        .unwrap();
    assert_eq!(got.len() as u64, w.next);
    assert_eq!(got.last().unwrap().payload.as_ref(), b"mine");
    c.stop_all().await;
}

/// Without peers, or with sync commits off, the default read is local
/// and asking for confirmed changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_peers_and_sync_off_serve_local() {
    let s = Server::start().await;
    let client = chron_client::Client::connect(s.addr(), Default::default())
        .await
        .unwrap();
    let w = client.commit(free("x")).await.unwrap();
    assert_eq!(
        client
            .read_all_with(0, 0, with(Consistency::Confirmed))
            .await
            .unwrap()
            .len() as u64,
        w.next
    );
    let h = client
        .head_with(with(Consistency::Confirmed))
        .await
        .unwrap();
    assert_eq!((h.head, h.confirmed, h.local), (w.next, w.next, w.next));
    assert_eq!(
        client.info().await.unwrap().read_consistency,
        Consistency::Local
    );
    s.stop().await;

    let mut c = Cluster::start_with(3, TTL, |o| o.sync_commit = SyncCommit::Off).await;
    let (l, others) = settled(&c).await;
    for i in &others {
        c.hold_apply(*i, true);
    }
    let client = c.client(l).await;
    let w = client.commit(free("async")).await.unwrap();
    assert_eq!(
        client.read_all(0, 0).await.unwrap().len() as u64,
        w.next,
        "sync off: the default read is local"
    );
    assert_eq!(
        client.info().await.unwrap().read_consistency,
        Consistency::Local
    );
    assert_eq!(
        client
            .read_all_with(0, 0, with(Consistency::Confirmed))
            .await
            .unwrap()
            .len(),
        0,
        "asked for, the confirmed bound still holds"
    );
    for i in &others {
        c.hold_apply(*i, false);
    }
    c.stop_all().await;
}
