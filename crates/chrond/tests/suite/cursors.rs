//! Durable cursors: the daemon keeps a subscription's position, the newest
//! subscription owns it, acknowledgments replicate, delivery is confirmed.

use std::time::Duration;

use chron_client::{ClientError, CommitOptions, NewEvent, Transaction};
use chron_proto::v1 as pb;
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

async fn typed(s: &Server) -> chron_client::Client {
    chron_client::Client::connect(s.addr(), Default::default())
        .await
        .unwrap()
}

async fn fill(client: &chron_client::Client, n: usize) -> u64 {
    let mut next = 0;
    for i in 0..n {
        next = client.commit(free(&format!("e{i}"))).await.unwrap().next;
    }
    next
}

async fn next_event(stream: &mut chron_client::EventStream) -> chron_client::Event {
    tokio::time::timeout(SOON, stream.next())
        .await
        .expect("an event in time")
        .expect("the stream is open")
        .expect("an event, not an error")
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
    (l, others)
}

/// A second subscription on the cursor resumes at the last
/// acknowledgment under the next session.
#[tokio::test(flavor = "multi_thread")]
async fn resume_after_reconnect() {
    let s = Server::start().await;
    let client = typed(&s).await;
    fill(&client, 5).await;
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!((cursor.session(), cursor.from), (1, 0));
    for i in 0..3 {
        assert_eq!(next_event(&mut stream).await.position, i);
    }
    assert_eq!(cursor.ack(3).await.unwrap(), 3);
    drop(stream);
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(
        (cursor.session(), cursor.from),
        (2, 3),
        "resumed at the acknowledgment"
    );
    assert_eq!(next_event(&mut stream).await.position, 3);
    let l = client.cursors().await.unwrap();
    assert_eq!(l.cursors.len(), 1);
    assert_eq!(
        (l.cursors[0].next, l.cursors[0].session, l.confirmed),
        (3, 2, 5)
    );
    s.stop().await;
}

/// The newest subscription ends the older one and owns the
/// acknowledgments.
#[tokio::test(flavor = "multi_thread")]
async fn takeover_ends_the_old_stream_and_refuses_its_acks() {
    let s = Server::start().await;
    let client = typed(&s).await;
    fill(&client, 2).await;
    let (mut first, c1) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(next_event(&mut first).await.position, 0);
    let (mut second, c2) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(c2.session(), 2);
    // The first stream ends; its buffered event may come out first.
    let mut ended = false;
    for _ in 0..3 {
        match tokio::time::timeout(SOON, first.next()).await.unwrap() {
            Some(Err(ClientError::Unavailable(m))) => {
                assert!(m.contains("taken over by session 2"), "{m}");
                ended = true;
                break;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert!(ended, "the first stream kept going");
    match c1.ack(1).await.unwrap_err() {
        ClientError::TakenOver { session: 2, .. } => {}
        e => panic!("not taken over: {e:?}"),
    }
    assert_eq!(next_event(&mut second).await.position, 0);
    assert_eq!(c2.ack(1).await.unwrap(), 1);
    s.stop().await;
}

/// Acknowledgments reach the followers; after the leader dies the next
/// leader resumes the cursor where it was and refuses the old session.
#[tokio::test(flavor = "multi_thread")]
async fn acks_replicate_and_survive_a_leader_kill() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    fill(&client, 5).await;
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    for _ in 0..4 {
        next_event(&mut stream).await;
    }
    assert_eq!(cursor.ack(4).await.unwrap(), 4);
    c.wait("the cursor on every follower", SOON, || {
        others.iter().all(|i| {
            c.node(*i)
                .default_db()
                .db
                .cursor("p")
                .unwrap()
                .is_some_and(|s| s.next == 4 && s.session == 1)
        })
    })
    .await;
    drop(stream);
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let client2 = c.client(l2).await;
    let (mut stream, cursor2) = client2.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(
        (cursor2.session(), cursor2.from),
        (2, 4),
        "{}",
        c.describe()
    );
    assert_eq!(next_event(&mut stream).await.position, 4);
    // The old session is refused on the new leader too.
    let mut raw = pb::chron_client::ChronClient::connect(c.addr(l2))
        .await
        .unwrap();
    let err = raw
        .ack(pb::AckRequest {
            cursor: "p".into(),
            next: 5,
            session: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
    assert_eq!(cursor2.ack(5).await.unwrap(), 5);
    c.stop_all().await;
}

/// A cursor subscription delivers only what a majority holds, and an
/// acknowledgment past that bound is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_delivers_confirmed_only() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    for i in &others {
        c.hold_apply(*i, true);
    }
    let held = client
        .commit_with(
            free("held"),
            CommitOptions {
                sync: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "an unconfirmed write was delivered on a cursor"
    );
    match cursor.ack(held.next).await.unwrap_err() {
        ClientError::InvalidArgument(m) => assert!(m.contains("past the confirmed bound"), "{m}"),
        e => panic!("not refused: {e:?}"),
    }
    c.hold_apply(others[0], false);
    assert_eq!(next_event(&mut stream).await.position, held.first);
    c.wait("confirmed", SOON, || {
        c.node(l).default_db().confirmed() >= held.next
    })
    .await;
    assert_eq!(cursor.ack(held.next).await.unwrap(), held.next);
    c.hold_apply(others[1], false);
    c.stop_all().await;
}

/// A cursor below the retention floor resumes at the floor.
#[tokio::test(flavor = "multi_thread")]
async fn below_floor_resumes_at_the_floor() {
    // Segments of a few records, so retention can drop some.
    let s = Server::start_with(|o| o.segment_max_bytes = Some(4096)).await;
    let client = typed(&s).await;
    for i in 0..60 {
        client.commit(free(&format!("{i:0>200}"))).await.unwrap();
    }
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    next_event(&mut stream).await;
    next_event(&mut stream).await;
    cursor.ack(2).await.unwrap();
    drop(stream);
    let db = s.running.as_ref().unwrap().db.clone();
    let floor = db.retain_from(40).unwrap().floor;
    assert!(floor > 2, "retention dropped segments: floor {floor}");
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(cursor.from, floor, "resumed at the floor, not at 2");
    assert_eq!(next_event(&mut stream).await.position, floor);
    assert_eq!(client.cursors().await.unwrap().cursors[0].next, floor);
    s.stop().await;
}

/// An acknowledgment that does not move the cursor answers the stored
/// position; a delete ends the live stream and forgets the cursor.
#[tokio::test(flavor = "multi_thread")]
async fn an_ack_below_next_is_a_noop_and_delete_ends_the_stream() {
    let s = Server::start().await;
    let client = typed(&s).await;
    fill(&client, 4).await;
    let (mut stream, cursor) = client.subscribe_cursor("p", 0, None).await.unwrap();
    assert_eq!(cursor.ack(3).await.unwrap(), 3);
    assert_eq!(cursor.ack(2).await.unwrap(), 3, "a lower ack is a no-op");
    assert!(
        cursor
            .ack(9)
            .await
            .unwrap_err()
            .to_string()
            .contains("past the confirmed bound")
    );
    match client.cursors().await.unwrap().cursors.as_slice() {
        [c] => assert_eq!((c.name.as_str(), c.next, c.session), ("p", 3, 1)),
        other => panic!("{other:?}"),
    }
    assert!(client.delete_cursor("p").await.unwrap());
    let mut ended = false;
    for _ in 0..5 {
        match tokio::time::timeout(SOON, stream.next()).await.unwrap() {
            Some(Err(ClientError::Unavailable(m))) => {
                assert!(m.contains("deleted"), "{m}");
                ended = true;
                break;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert!(ended);
    assert!(!client.delete_cursor("p").await.unwrap());
    assert!(client.cursors().await.unwrap().cursors.is_empty());
    match cursor.ack(3).await.unwrap_err() {
        ClientError::NotFound(_) => {}
        e => panic!("{e:?}"),
    }
    match client.subscribe_cursor("Not Valid", 0, None).await {
        Err(ClientError::InvalidArgument(_)) => {}
        Err(e) => panic!("{e:?}"),
        Ok(_) => panic!("an invalid cursor name was accepted"),
    }
    s.stop().await;
}

/// A follower refuses cursor subscriptions and acknowledgments.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_keeps_no_cursors() {
    let mut c = Cluster::start(3, TTL).await;
    let (_, others) = settled(&c).await;
    let fc = c.client(others[0]).await;
    match fc.subscribe_cursor("p", 0, None).await {
        Err(ClientError::Fenced { message, .. }) => {
            assert!(message.contains("kept on the leader"), "{message}")
        }
        Err(e) => panic!("{e:?}"),
        Ok(_) => panic!("a follower accepted a cursor subscription"),
    }
    c.stop_all().await;
}
