use chron_proto::v1 as pb;
use tokio_stream::StreamExt;
use tonic::Code;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn limit_zero_streams_everything_across_pages() {
    let mut s = Server::start().await;
    for i in 0..600u64 {
        let mut t = s.running.as_ref().unwrap().db.transaction();
        t.append_one(
            chron::Uuid::from_u128(1),
            chron::Expected::Any,
            chron::NewEvent::new(format!("e{i}")).tag("t"),
        );
        t.commit().unwrap();
    }
    let got = collect(
        s.client
            .read_aggregate(pb::ReadAggregateRequest {
                aggregate: agg(1).as_bytes().to_vec(),
                from_index: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(got.len(), 600);
    assert_eq!(
        got.iter().map(|e| e.index).collect::<Vec<_>>(),
        (0..600).collect::<Vec<_>>()
    );
    let back = collect(
        s.client
            .read_aggregate(pb::ReadAggregateRequest {
                aggregate: agg(1).as_bytes().to_vec(),
                from_index: u64::MAX,
                limit: 3,
                backward: true,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(
        back.iter().map(|e| e.index).collect::<Vec<_>>(),
        [599, 598, 597]
    );
    let all_back = collect(
        s.client
            .read_all(pb::ReadAllRequest {
                from: u64::MAX,
                limit: 0,
                backward: true,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(all_back.len(), 600);
    assert_eq!(all_back.last().unwrap().position, 0);
    let tagged = collect(
        s.client
            .read_tag(pb::ReadTagRequest {
                tag: b"t".to_vec(),
                from: 590,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(positions(&tagged), (590..600).collect::<Vec<_>>());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn read_all_from_position_and_past_head_is_out_of_range() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 4).await;
    let got = collect(
        s.client
            .read_all(pb::ReadAllRequest {
                from: 2,
                limit: 1,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(positions(&got), [2]);
    let err = s
        .client
        .read_all(pb::ReadAllRequest {
            from: 5,
            limit: 1,
            backward: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::OutOfRange);
    let empty = collect(
        s.client
            .read_all(pb::ReadAllRequest {
                from: 4,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        4
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_stream_midway_leaves_the_server_usable() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 300).await;
    let mut stream = s
        .client
        .read_all(pb::ReadAllRequest {
            from: 0,
            limit: 0,
            backward: false,
        })
        .await
        .unwrap()
        .into_inner();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.position, 0);
    drop(stream);
    fill(&mut s.client, agg(1), 1).await;
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        301
    );
    s.stop().await;
}
