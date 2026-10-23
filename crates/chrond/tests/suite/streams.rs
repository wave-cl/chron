//! Streams on the daemon: defined on the leader, read everywhere,
//! replicated with the chunks, followed by cursors, carried by backups.

use std::time::Duration;

use chron_client::{ClientError, NewEvent, Source, Transaction};
use chrond::testing::{Cluster, local_read};
use tokio_stream::StreamExt;

use crate::common::{Server, with_backup_dir};

const TTL: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_secs(6);

fn tagged(payload: &str, tags: &[&str]) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![
        NewEvent::new(payload.to_string()).tags(tags.iter().map(|t| t.to_string())),
    ]);
    t
}

/// Commits `n` free events whose tags cycle a, b, c, (none).
async fn fill_tagged(client: &chron_client::Client, n: usize) -> u64 {
    let mut next = 0;
    for i in 0..n {
        let tags: &[&str] = match i % 4 {
            0 => &["a"],
            1 => &["b"],
            2 => &["c"],
            _ => &[],
        };
        next = client
            .commit(tagged(&format!("e{i}"), tags))
            .await
            .unwrap()
            .next;
    }
    next
}

async fn typed(s: &Server) -> chron_client::Client {
    chron_client::Client::connect(s.addr(), Default::default())
        .await
        .unwrap()
}

/// The stream's positions as the node holds them (a local read: a fresh
/// follower's confirmed bound may lag).
async fn held(client: &chron_client::Client, name: &str) -> Result<Vec<u64>, ClientError> {
    Ok(client
        .read_stream_with(name, 0, 0, local_read())
        .await?
        .iter()
        .map(|e| e.position)
        .collect())
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

/// Polls `f` until it answers `Ok(true)`, for `SOON`.
async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + SOON;
    loop {
        if f().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A stream defined on the leader is read on every follower with the
/// same positions; a follower that was away takes the leader's list on
/// the opening chunk, dropping a stream the leader removed meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_defined_on_the_leader_reaches_every_node() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    fill_tagged(&client, 12).await;
    let info = client.create_stream("ab", ["a", "b"]).await.unwrap();
    assert!(info.ready);
    assert_eq!(info.tags, vec![chron_client::Bytes::from("a"), "b".into()]);
    let empty = client.create_stream("x", ["x"]).await.unwrap();
    assert!(empty.ready);
    let want = held(&client, "ab").await.unwrap();
    assert_eq!(want, [0, 1, 4, 5, 8, 9]);
    assert!(held(&client, "x").await.unwrap().is_empty());
    for f in &others {
        let fc = c.client(*f).await;
        eventually("the follower holds the stream", || async {
            held(&fc, "ab").await.ok() == Some(want.clone())
        })
        .await;
        // The follower fills off its apply path: ready a moment later.
        eventually("the empty stream is ready on the follower", || async {
            held(&fc, "x").await.is_ok()
        })
        .await;
        assert!(held(&fc, "x").await.unwrap().is_empty());
    }
    // Live members reach the followers' streams too.
    fill_tagged(&client, 4).await;
    let want = held(&client, "ab").await.unwrap();
    assert_eq!(want.len(), 8);
    for f in &others {
        let fc = c.client(*f).await;
        eventually("the follower keeps up", || async {
            held(&fc, "ab").await.ok() == Some(want.clone())
        })
        .await;
    }
    // Away while the leader removes `x`: the opening chunk's complete
    // list drops it.
    let f = others[0];
    c.stop(f).await;
    assert!(client.delete_stream("x").await.unwrap());
    fill_tagged(&client, 2).await;
    c.restart(f).await;
    c.wait_following(f, l, SOON).await;
    let fc = c.client(f).await;
    eventually("the removed stream goes", || async {
        let names: Vec<String> = fc
            .streams()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.name)
            .collect();
        names == ["ab"]
    })
    .await;
    assert!(matches!(
        fc.read_stream_with("x", 0, 0, local_read()).await,
        Err(ClientError::NotFound(_))
    ));
}

/// A stream deleted and defined again with other tags while a follower
/// held its chunks: the follower ends up with the second definition.
#[tokio::test(flavor = "multi_thread")]
async fn a_redefinition_between_chunks_reaches_a_held_follower() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let f = others[0];
    let client = c.client(l).await;
    fill_tagged(&client, 8).await;
    c.hold_apply(f, true);
    client.create_stream("s", ["a"]).await.unwrap();
    assert!(client.delete_stream("s").await.unwrap());
    client.create_stream("s", ["c"]).await.unwrap();
    fill_tagged(&client, 4).await;
    let want = held(&client, "s").await.unwrap();
    assert_eq!(want, [2, 6, 10]);
    c.hold_apply(f, false);
    let fc = c.client(f).await;
    eventually("the follower holds the second definition", || async {
        let streams = fc.streams().await.unwrap_or_default();
        streams.len() == 1
            && streams[0].tags == vec![chron_client::Bytes::from("c")]
            && streams[0].ready
            && held(&fc, "s").await.ok() == Some(want.clone())
    })
    .await;
    // Away while the stream is redefined: the opening chunk lists the
    // name with other tags, and the copy it holds is replaced.
    c.stop(f).await;
    assert!(client.delete_stream("s").await.unwrap());
    client.create_stream("s", ["b"]).await.unwrap();
    let want = held(&client, "s").await.unwrap();
    assert_eq!(want, [1, 5, 9]);
    c.restart(f).await;
    c.wait_following(f, l, SOON).await;
    let fc = c.client(f).await;
    eventually("the follower holds the third definition", || async {
        let streams = fc.streams().await.unwrap_or_default();
        streams.len() == 1
            && streams[0].tags == vec![chron_client::Bytes::from("b")]
            && streams[0].ready
            && held(&fc, "s").await.ok() == Some(want.clone())
    })
    .await;
}

/// Deleting a stream ends a subscription on it; an unknown or existing
/// name is answered as such.
#[tokio::test(flavor = "multi_thread")]
async fn delete_ends_a_subscription_and_names_are_checked() {
    let s = Server::start().await;
    let client = typed(&s).await;
    fill_tagged(&client, 8).await;
    assert!(matches!(
        client.read_stream("none", 0, 0).await,
        Err(ClientError::NotFound(_))
    ));
    assert!(matches!(
        client.create_stream("Bad Name", ["a"]).await,
        Err(ClientError::InvalidArgument(_))
    ));
    client.create_stream("s", ["a", "b"]).await.unwrap();
    assert!(matches!(
        client.create_stream("s", ["c"]).await,
        Err(ClientError::AlreadyExists(_))
    ));
    let mut stream = client.subscribe_stream("s", 0).await.unwrap();
    assert_eq!(next_event(&mut stream).await.position, 0);
    assert_eq!(next_event(&mut stream).await.position, 1);
    assert!(client.delete_stream("s").await.unwrap());
    // What the subscription had already fetched goes out (members all),
    // then the stream ends.
    let mut ended = false;
    for _ in 0..16 {
        match tokio::time::timeout(SOON, stream.next()).await.unwrap() {
            Some(Ok(ev)) => assert!(ev.position % 4 < 2, "a member: {ev:?}"),
            Some(Err(ClientError::Unavailable(m))) => {
                assert!(m.contains("deleted"), "{m}");
                ended = true;
                break;
            }
            other => panic!("expected the stream to end, got {other:?}"),
        }
    }
    assert!(ended, "the stream ended");
    assert!(!client.delete_stream("s").await.unwrap());
    assert!(client.streams().await.unwrap().is_empty());
    assert_eq!(client.info().await.unwrap().streams, 0);
}

/// A cursor on a stream resumes on the next leader at its acknowledgment.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_on_a_stream_resumes_across_a_leader_kill() {
    let mut c = Cluster::start(3, TTL).await;
    let (l, others) = settled(&c).await;
    let client = c.client(l).await;
    fill_tagged(&client, 12).await;
    client.create_stream("s", ["a"]).await.unwrap();
    let (mut stream, cursor) = client
        .subscribe_cursor_source("p", 0, Source::Stream("s".into()))
        .await
        .unwrap();
    assert_eq!(next_event(&mut stream).await.position, 0);
    assert_eq!(next_event(&mut stream).await.position, 4);
    assert_eq!(cursor.ack(5).await.unwrap(), 5);
    c.wait("the cursor on every follower", SOON, || {
        others.iter().all(|i| {
            c.node(*i)
                .default_db()
                .db
                .cursor("p")
                .unwrap()
                .is_some_and(|s| s.next == 5 && s.session == 1)
        })
    })
    .await;
    drop(stream);
    c.stop(l).await;
    let l2 = c.wait_for_leader(SOON).await;
    let client2 = c.client(l2).await;
    eventually("the next leader holds the stream", || async {
        client2
            .streams()
            .await
            .map(|v| v.len() == 1 && v[0].ready)
            .unwrap_or(false)
    })
    .await;
    let (mut stream, cursor) = client2
        .subscribe_cursor_source("p", 0, Source::Stream("s".into()))
        .await
        .unwrap();
    assert_eq!(
        (cursor.session(), cursor.from),
        (2, 5),
        "resumed at the acknowledgment"
    );
    assert_eq!(next_event(&mut stream).await.position, 8);
}

/// Streams are defined on the leader only.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_refuses_to_define_a_stream() {
    let c = Cluster::start(3, TTL).await;
    let (_, others) = settled(&c).await;
    let fc = c.client(others[0]).await;
    assert!(matches!(
        fc.create_stream("s", ["a"]).await,
        Err(ClientError::Fenced { .. })
    ));
    assert!(matches!(
        fc.delete_stream("s").await,
        Err(ClientError::Fenced { .. })
    ));
}

/// A backup fetched over the daemon carries the definitions: a daemon
/// started on the restored copy serves the stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_fetched_backup_restores_the_definitions() {
    let s = Server::start_with(with_backup_dir).await;
    let client = typed(&s).await;
    fill_tagged(&client, 12).await;
    client.create_stream("s", ["a", "c"]).await.unwrap();
    let want = held(&client, "s").await.unwrap();
    let archive = s.dir.path().join("fetched.chronbak");
    let meta = client.fetch_backup_to(&archive).await.unwrap();
    assert_eq!(meta.format, 3);
    let dir = tempfile::tempdir().unwrap();
    chron::restore_backup(
        &archive,
        dir.path().join("data/default"),
        &chron::Options::new(),
    )
    .unwrap();
    let s2 = Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    let client2 = typed(&s2).await;
    let streams = client2.streams().await.unwrap();
    assert_eq!(streams.len(), 1);
    assert!(streams[0].ready);
    assert_eq!(held(&client2, "s").await.unwrap(), want);
}
