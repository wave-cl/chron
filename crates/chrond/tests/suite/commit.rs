use chron_proto::v1 as pb;
use tonic::Code;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn commit_two_aggregates_and_free_events_returns_positions_and_results() {
    let mut s = Server::start().await;
    let resp = s
        .client
        .commit(pb::CommitRequest {
            appends: vec![
                append(
                    Some(agg(1)),
                    new(),
                    vec![new_event("a0", &["t"]), new_event("a1", &[])],
                ),
                append(None, None, vec![new_event("free", &["t"])]),
                append(Some(agg(2)), exact(0), vec![new_event("b0", &[])]),
            ],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((resp.first, resp.last, resp.next), (0, 3, 4));
    assert_eq!(resp.appends.len(), 3);
    assert_eq!(resp.appends[0].aggregate, Some(agg(1).as_bytes().to_vec()));
    assert_eq!(resp.appends[0].new_len, 2);
    assert_eq!(resp.appends[1].aggregate, None);
    assert_eq!(resp.appends[1].first_position, 2);
    assert_eq!(resp.appends[2].first_index, 0);
    let all = collect(
        s.client
            .read_all(pb::ReadAllRequest {
                from: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(payloads(&all), ["a0", "a1", "free", "b0"]);
    assert!(all[0].first_in_txn && !all[0].last_in_txn);
    assert!(all[3].last_in_txn);
    assert_eq!(all[2].aggregate, None);
    assert_eq!(all[0].tags, vec![b"t".to_vec()]);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_is_failed_precondition_with_headers_and_nothing_written() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 1).await;
    let err = s
        .client
        .commit(pb::CommitRequest {
            appends: vec![
                append(Some(agg(2)), new(), vec![new_event("b", &[])]),
                append(Some(agg(1)), exact(5), vec![new_event("a", &[])]),
            ],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        err.metadata()
            .get(chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER)
            .unwrap(),
        "1"
    );
    assert_eq!(
        err.metadata()
            .get(chron_proto::CONFLICT_AGGREGATE_HEADER)
            .unwrap(),
        agg(1).to_string().as_str()
    );
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        1
    );
    let len = s
        .client
        .aggregate_len(pb::AggregateLenRequest {
            aggregate: agg(2).as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .len;
    assert_eq!(len, 0);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_requests_are_invalid_argument() {
    let mut s = Server::start().await;
    let cases = vec![
        pb::CommitRequest {
            appends: vec![],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        },
        pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        },
        pb::CommitRequest {
            appends: vec![append(None, exact(0), vec![new_event("f", &[])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        },
        pb::CommitRequest {
            appends: vec![pb::Append {
                aggregate: Some(vec![1u8; 17]),
                expected: any(),
                events: vec![new_event("x", &[])],
            }],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        },
        pb::CommitRequest {
            appends: vec![append(
                Some(agg(1)),
                any(),
                vec![pb::NewEvent {
                    tags: vec![vec![b'x'; 256]],
                    metadata: vec![],
                    payload: b"x".to_vec(),
                }],
            )],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        },
    ];
    for req in cases {
        let err = s.client.commit(req).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    }
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        0
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resent_commit_id_is_a_duplicate_over_grpc() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 1).await;
    let req = || pb::CommitRequest {
        appends: vec![
            append(
                Some(agg(1)),
                exact(1),
                vec![new_event("a1", &[]), new_event("a2", &[])],
            ),
            append(None, None, vec![new_event("free", &["t"])]),
        ],
        epoch: None,
        sync: None,
        commit_id: vec![7; 16],
    };
    let first = s.client.commit(req()).await.unwrap().into_inner();
    assert_eq!((first.first, first.next, first.duplicate), (1, 4, false));
    let again = s.client.commit(req()).await.unwrap().into_inner();
    assert_eq!((again.first, again.last, again.next), (1, 3, 4));
    assert!(again.duplicate);
    assert_eq!(again.appends, first.appends);
    // The stale expectation is not re-checked on a resend.
    let stale = s.client.commit(req()).await.unwrap().into_inner();
    assert!(stale.duplicate);
    let all = collect(
        s.client
            .read_all(pb::ReadAllRequest {
                from: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 4, "nothing was written twice");
    assert_eq!(all[1].commit_id.as_deref(), Some(&[7u8; 16][..]));
    assert_eq!(all[2].commit_id, None);
    assert_eq!(all[0].commit_id, None, "fill sends no id over raw gRPC");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_commit_id_is_invalid_argument() {
    let mut s = Server::start().await;
    let mut req = pb::CommitRequest {
        appends: vec![append(Some(agg(1)), any(), vec![new_event("a", &[])])],
        epoch: None,
        sync: None,
        commit_id: vec![1; 15],
    };
    let err = s.client.commit(req.clone()).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("16 bytes"), "{err}");
    req.commit_id = vec![1; 16];
    s.client.commit(req.clone()).await.unwrap();
    // The same id on another shape.
    req.appends = vec![append(Some(agg(2)), any(), vec![new_event("b", &[])])];
    let err = s.client.commit(req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("different shape"), "{err}");
    let all = read_all_payloads(&mut s.client).await;
    assert_eq!(all, ["a"]);
    s.stop().await;
}
