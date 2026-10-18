use std::time::Duration;

use chron_proto::v1 as pb;
use tokio_stream::StreamExt;

use crate::common::*;

const LIVE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_catches_up_then_streams_live() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 3).await;
    let mut stream = s
        .client
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(
            tokio::time::timeout(LIVE, stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(positions(&got), [0, 1, 2]);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err()
    );
    let db = s.running.as_ref().unwrap().db.clone();
    tokio::task::spawn_blocking(move || {
        let mut t = db.transaction();
        t.append_one(
            chron::Uuid::from_u128(1),
            chron::Expected::Any,
            chron::NewEvent::new("live"),
        );
        t.append_free(vec![chron::NewEvent::new("free").tag("t")]);
        t.commit().unwrap();
    })
    .await
    .unwrap();
    let live = tokio::time::timeout(LIVE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((live.position, live.first_in_txn), (3, true));
    let free = tokio::time::timeout(LIVE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        (free.position, free.aggregate.is_none(), free.last_in_txn),
        (4, true, true)
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_tag_filters_and_from_past_head_waits() {
    let mut s = Server::start().await;
    s.client
        .commit(pb::CommitRequest {
            appends: vec![append(
                Some(agg(1)),
                any(),
                vec![
                    new_event("a", &["t"]),
                    new_event("b", &[]),
                    new_event("c", &["t"]),
                ],
            )],
        })
        .await
        .unwrap();
    let mut tagged = s
        .client
        .subscribe(pb::SubscribeRequest {
            from: 0,
            tag: Some(b"t".to_vec()),
        })
        .await
        .unwrap()
        .into_inner();
    let a = tokio::time::timeout(LIVE, tagged.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let c = tokio::time::timeout(LIVE, tagged.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((a.position, c.position), (0, 2));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), tagged.next())
            .await
            .is_err()
    );

    let mut ahead = s
        .client
        .subscribe(pb::SubscribeRequest { from: 5, tag: None })
        .await
        .unwrap()
        .into_inner();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), ahead.next())
            .await
            .is_err()
    );
    fill(&mut s.client, agg(2), 3).await;
    let first = tokio::time::timeout(LIVE, ahead.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.position, 5);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_stream_ends_on_shutdown() {
    let mut s = Server::start().await;
    let mut stream = s
        .client
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await
        .unwrap()
        .into_inner();
    let running = s.running.take().unwrap();
    let stopped = tokio::spawn(running.shutdown());
    let end = tokio::time::timeout(LIVE, stream.next()).await.unwrap();
    assert!(end.is_none() || end.unwrap().is_err(), "the stream ends");
    stopped.await.unwrap().unwrap();
}
