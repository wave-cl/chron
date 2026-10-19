//! The metrics endpoint and what it reports.

use std::io::{Read, Write};
use std::time::Duration;

use chron_proto::v1 as pb;
use tokio_stream::StreamExt;

use crate::common::*;

/// `GET path` over a raw socket (no HTTP client dependency); returns the
/// status line's code and the body.
fn http_get(addr: std::net::SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    let code: u16 = raw
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (code, body)
}

fn scrape(addr: std::net::SocketAddr) -> String {
    let (code, body) = http_get(addr, "GET", "/metrics");
    assert_eq!(code, 200);
    body
}

/// The value of the sample `name{labels}` (labels given as `k="v"`
/// pairs in any order), if present.
fn metric(body: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    body.lines().find_map(|line| {
        let (head, value) = line.rsplit_once(' ')?;
        let (n, rest) = match head.split_once('{') {
            Some((n, rest)) => (n, rest.trim_end_matches('}')),
            None => (head, ""),
        };
        if n != name {
            return None;
        }
        let present: Vec<&str> = rest.split(',').collect();
        for (k, v) in labels {
            let want = format!("{k}=\"{v}\"");
            if !present.iter().any(|p| *p == want) {
                return None;
            }
        }
        value.parse().ok()
    })
}

fn with_metrics(o: &mut chrond::Options) {
    o.metrics_listen = Some("127.0.0.1:0".parse().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_endpoint_reports_calls_commits_and_gauges() {
    let mut s = Server::start_with(with_metrics).await;
    let maddr = s
        .running
        .as_ref()
        .unwrap()
        .metrics_addr
        .expect("metrics listener");
    fill(&mut s.client, agg(1), 2).await;
    let err = s
        .client
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), exact(7), vec![new_event("x", &[])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let dup = pb::CommitRequest {
        appends: vec![append(Some(agg(1)), any(), vec![new_event("d", &[])])],
        epoch: None,
        sync: None,
        commit_id: vec![4; 16],
    };
    assert!(
        !s.client
            .commit(dup.clone())
            .await
            .unwrap()
            .into_inner()
            .duplicate
    );
    assert!(s.client.commit(dup).await.unwrap().into_inner().duplicate);
    let all = read_all_payloads(&mut s.client).await;
    assert_eq!(all.len(), 3);
    let mut sub = s
        .client
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(sub.next().await.unwrap().unwrap().position, 0);

    let maddr2 = maddr;
    let body = tokio::task::spawn_blocking(move || scrape(maddr2))
        .await
        .unwrap();
    let commit = "/chron.v1.Chron/Commit";
    let d = ("db", "default");
    assert_eq!(
        metric(
            &body,
            "chron_rpc_calls_total",
            &[d, ("method", commit), ("code", "OK")]
        ),
        Some(4.0),
        "{body}"
    );
    assert_eq!(
        metric(
            &body,
            "chron_rpc_calls_total",
            &[d, ("method", commit), ("code", "FailedPrecondition")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric(
            &body,
            "chron_rpc_calls_total",
            &[d, ("method", "/chron.v1.Chron/ReadAll"), ("code", "OK")]
        ),
        Some(1.0),
        "a drained stream counts once, at its end"
    );
    assert_eq!(
        metric(&body, "chron_commits_total", &[d, ("result", "ok")]),
        Some(3.0)
    );
    assert_eq!(
        metric(&body, "chron_commits_total", &[d, ("result", "duplicate")]),
        Some(1.0)
    );
    assert_eq!(
        metric(&body, "chron_commits_total", &[d, ("result", "conflict")]),
        Some(1.0)
    );
    assert_eq!(
        metric(
            &body,
            "chron_rpc_duration_seconds_count",
            &[d, ("method", commit)]
        ),
        Some(5.0)
    );
    assert_eq!(
        metric(&body, "chron_events_committed_total", &[d]),
        Some(3.0)
    );
    assert_eq!(metric(&body, "chron_head", &[d]), Some(3.0));
    assert_eq!(metric(&body, "chron_floor", &[d]), Some(0.0));
    assert_eq!(
        metric(&body, "chron_role", &[d, ("role", "leader")]),
        Some(1.0)
    );
    assert_eq!(
        metric(&body, "chron_role", &[d, ("role", "replica")]),
        Some(0.0)
    );
    assert_eq!(
        metric(
            &body,
            "chron_build_info",
            &[("version", env!("CARGO_PKG_VERSION"))]
        ),
        Some(1.0)
    );
    assert!(metric(&body, "chron_start_time_seconds", &[]).unwrap() > 1.6e9);
    assert_eq!(
        metric(
            &body,
            "chron_streams_open",
            &[d, ("method", "/chron.v1.Chron/Subscribe")]
        ),
        Some(1.0),
        "the subscription is open"
    );
    assert!(
        !body.contains("chron_replication_lag_events") && !body.contains("chron_replica_connected"),
        "a leader reports no replica gauges"
    );
    drop(sub);
    wait_until("the subscription to be counted closed", || async move {
        let body = tokio::task::spawn_blocking(move || scrape(maddr2))
            .await
            .unwrap();
        metric(
            &body,
            "chron_streams_open",
            &[("db", "default"), ("method", "/chron.v1.Chron/Subscribe")],
        ) == Some(0.0)
    })
    .await;

    // The endpoint itself.
    let (code, _) = tokio::task::spawn_blocking(move || http_get(maddr2, "GET", "/nope"))
        .await
        .unwrap();
    assert_eq!(code, 404);
    let (code, _) = tokio::task::spawn_blocking(move || http_get(maddr2, "POST", "/metrics"))
        .await
        .unwrap();
    assert_eq!(code, 405);
    let (code, body) = tokio::task::spawn_blocking(move || http_get(maddr2, "GET", "/"))
        .await
        .unwrap();
    assert_eq!((code, body.trim()), (200, "chrond metrics at /metrics"));
    s.stop().await;

    let s = Server::start().await;
    assert!(
        s.running.as_ref().unwrap().metrics_addr.is_none(),
        "off by default"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_reports_lag_connection_and_errors() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 3).await;
    let addr = leader.addr();
    let replica = Server::start_with(move |o| {
        with_metrics(o);
        o.replicate_from = Some(addr);
    })
    .await;
    let maddr = replica.running.as_ref().unwrap().metrics_addr.unwrap();
    wait_until("the replica to catch up", || async move {
        let body = tokio::task::spawn_blocking(move || scrape(maddr))
            .await
            .unwrap();
        metric(&body, "chron_replication_lag_events", &[("db", "default")]) == Some(0.0)
            && metric(&body, "chron_replica_connected", &[("db", "default")]) == Some(1.0)
    })
    .await;
    // Lag is leader_head - head; force the leader's head as the puller
    // would record it.
    replica
        .running
        .as_ref()
        .unwrap()
        .node
        .default_db()
        .record_replica(|r| r.leader_head = Some(10));
    let body = tokio::task::spawn_blocking(move || scrape(maddr))
        .await
        .unwrap();
    assert_eq!(
        metric(&body, "chron_replication_lag_events", &[("db", "default")]),
        Some(7.0)
    );
    assert_eq!(
        metric(
            &body,
            "chron_role",
            &[("db", "default"), ("role", "replica")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric(
            &body,
            "chron_replication_chunks_applied_total",
            &[("db", "default")]
        ),
        Some(1.0)
    );
    leader.stop().await;
    wait_until("the replica to report the loss", || async move {
        let body = tokio::task::spawn_blocking(move || scrape(maddr))
            .await
            .unwrap();
        metric(&body, "chron_replica_connected", &[("db", "default")]) == Some(0.0)
            && metric(
                &body,
                "chron_replication_errors_total",
                &[("db", "default")],
            )
            .is_some_and(|n| n >= 1.0)
    })
    .await;
    replica.stop().await;
}
