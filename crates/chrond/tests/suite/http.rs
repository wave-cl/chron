//! The HTTP/JSON gateway, driven over raw sockets.

use std::io::{BufRead, BufReader, Read, Write};
use std::time::Duration;

use serde_json::{Value, json};

use crate::common::*;

/// One HTTP/1.1 request; returns the status code and the body (parsed
/// as JSON when it is).
pub(crate) fn http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, Value, String) {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            b.len()
        ));
    }
    req.push_str("\r\n");
    // The server may answer and close before an oversize body is fully
    // sent: a write error then is the answer arriving early, and a read
    // error after the response is the close.
    let _ = s.write_all(req.as_bytes());
    if let Some(b) = body {
        let _ = s.write_all(b.as_bytes());
    }
    let mut bytes = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => bytes.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let raw = String::from_utf8_lossy(&bytes).into_owned();
    let code: u16 = raw
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let text = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    let value = serde_json::from_str(&text).unwrap_or(Value::Null);
    (code, value, text)
}

pub(crate) async fn call(
    addr: std::net::SocketAddr,
    method: &'static str,
    path: String,
    headers: Vec<(&'static str, String)>,
    body: Option<String>,
) -> (u16, Value, String) {
    tokio::task::spawn_blocking(move || {
        let h: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        http(addr, method, &path, &h, body.as_deref())
    })
    .await
    .unwrap()
}

fn with_http(o: &mut chrond::Options) {
    o.http_listen = Some("127.0.0.1:0".parse().unwrap());
}

const AGG: &str = "00000000-0000-0000-0000-000000000001";

#[tokio::test(flavor = "multi_thread")]
async fn commit_then_read_in_the_cli_shape() {
    let s = Server::start_with(with_http).await;
    let addr = s
        .running
        .as_ref()
        .unwrap()
        .http_addr
        .expect("http listener");
    let txn = json!({"appends": [
        {"aggregate": AGG, "expect": "new", "events": [{"payload": "placed", "tags": ["orders"]}, {"payload_base64": "/w=="}]},
        {"aggregate": "free", "events": [{"payload": "note", "metadata": "m"}]}
    ]})
    .to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(txn),
    )
    .await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(
        (v["first"].as_u64(), v["last"].as_u64(), v["next"].as_u64()),
        (Some(0), Some(2), Some(3))
    );
    assert_eq!(v["appends"][0]["new_len"], 2);
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?from=0&limit=10".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let events = v["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["payload"], "placed");
    assert_eq!(events[0]["tags"][0], "orders");
    assert_eq!(events[0]["first_in_txn"], true);
    assert_eq!(events[1]["payload_base64"], "/w==");
    assert_eq!(events[2]["aggregate"], Value::Null);
    assert_eq!(events[2]["metadata"], "m");
    assert_eq!(v["next"], 3);
    let (code, v, _) = call(
        addr,
        "GET",
        format!("/v1/db/default/aggregates/{AGG}/events"),
        vec![],
        None,
    )
    .await;
    assert_eq!((code, v["events"].as_array().unwrap().len()), (200, 2));
    let (_, v, _) = call(
        addr,
        "GET",
        format!("/v1/db/default/aggregates/{AGG}/len"),
        vec![],
        None,
    )
    .await;
    assert_eq!(v["len"], 2);
    let (_, v, _) = call(addr, "GET", "/v1/db/default/head".into(), vec![], None).await;
    assert_eq!(v["head"], 3);
    let (_, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/tags/orders/events".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(v["events"].as_array().unwrap().len(), 1);
    let (_, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?backward=true&limit=2".into(),
        vec![],
        None,
    )
    .await;
    let positions: Vec<u64> = v["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["position"].as_u64().unwrap())
        .collect();
    assert_eq!(positions, [2, 1]);
    assert_eq!(v["next"], 0);
    let (_, v, _) = call(addr, "GET", "/v1/db/default/info".into(), vec![], None).await;
    assert_eq!(
        (
            v["database"].as_str(),
            v["role"].as_str(),
            v["head"].as_u64()
        ),
        (Some("default"), Some("leader"), Some(3))
    );
    assert!(v["db_id"].as_str().unwrap().len() == 36);
    let (_, v, _) = call(addr, "GET", "/v1/status".into(), vec![], None).await;
    assert_eq!(
        (v["role"].as_str(), v["head"].as_u64()),
        (Some("leader"), Some(3))
    );
    let (code, _, _) = call(addr, "POST", "/v1/db/default/flush".into(), vec![], None).await;
    assert_eq!(code, 204);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_carry_codes_and_metadata() {
    let s = Server::start_with(with_http).await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let ok = json!({"appends": [{"aggregate": AGG, "expect": "new", "events": [{"payload": "a"}, {"payload": "b"}]}]}).to_string();
    assert_eq!(
        call(
            addr,
            "POST",
            "/v1/db/default/commit".into(),
            vec![],
            Some(ok)
        )
        .await
        .0,
        200
    );
    let conflict =
        json!({"appends": [{"aggregate": AGG, "expect": 7, "events": [{"payload": "c"}]}]})
            .to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(conflict),
    )
    .await;
    assert_eq!(
        (code, v["code"].as_str(), v["actual_length"].as_u64()),
        (409, Some("FailedPrecondition"), Some(2))
    );
    assert_eq!(v["aggregate"], AGG);
    let (code, v, _) = call(addr, "GET", "/v1/db/nope/head".into(), vec![], None).await;
    assert_eq!((code, v["code"].as_str()), (404, Some("NotFound")));
    let (code, v, _) = call(addr, "GET", "/v1/db/Bad!/head".into(), vec![], None).await;
    assert_eq!((code, v["code"].as_str()), (400, Some("InvalidArgument")));
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?from=99".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(
        (
            code,
            v["code"].as_str(),
            v["position"].as_u64(),
            v["head"].as_u64()
        ),
        (416, Some("OutOfRange"), Some(99), Some(2))
    );
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?limit=abc".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!((code, v["code"].as_str()), (400, Some("InvalidArgument")));
    let (code, _, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some("{not json".into()),
    )
    .await;
    assert_eq!(code, 400);
    let (code, _, _) = call(
        addr,
        "GET",
        "/v1/db/default/aggregates/not-a-uuid/len".to_string(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 400);
    let free_expect =
        json!({"appends": [{"aggregate": "free", "expect": 3, "events": [{"payload": "x"}]}]})
            .to_string();
    let (code, _, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(free_expect),
    )
    .await;
    assert_eq!(code, 400);
    let (code, v, _) = call(addr, "PUT", "/v1/db/default/head".into(), vec![], None).await;
    assert_eq!((code, v["code"].as_str()), (405, Some("Unimplemented")));
    let (code, v, _) = call(addr, "GET", "/v1/nope".into(), vec![], None).await;
    assert_eq!((code, v["code"].as_str()), (404, Some("NotFound")));
    let stale = json!({"appends": [{"aggregate": AGG, "events": [{"payload": "d"}]}]}).to_string();
    s.running.as_ref().unwrap().db.set_epoch(2).unwrap();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit?epoch=1".into(),
        vec![],
        Some(stale),
    )
    .await;
    assert_eq!((code, v["epoch"].as_u64()), (409, Some(2)));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn oversize_bodies_are_413() {
    let s = Server::start_with(|o| {
        with_http(o);
        o.max_record_bytes = Some(512 * 1024);
        o.max_message_bytes = 1024 * 1024;
    })
    .await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    // The body limit allows a third more than the message limit, since
    // JSON carries binary as base64; this is above that.
    let big = json!({"appends": [{"aggregate": "free", "events": [{"payload": "x".repeat(1500 * 1024)}]}]}).to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(big),
    )
    .await;
    assert_eq!(
        (code, v["code"].as_str()),
        (413, Some("ResourceExhausted")),
        "{v}"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_gate_v1_and_admin_routes() {
    let s = Server::start_with_no_client(|o| {
        with_http(o);
        o.token = Some("user".into());
        o.admin_token = Some("admin".into());
    })
    .await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let (code, v, _) = call(addr, "GET", "/v1/db/default/head".into(), vec![], None).await;
    assert_eq!((code, v["code"].as_str()), (401, Some("Unauthenticated")));
    let (code, _, _) = call(
        addr,
        "GET",
        "/v1/db/default/head".into(),
        vec![("Authorization", "Bearer nope".into())],
        None,
    )
    .await;
    assert_eq!(code, 401);
    let (code, _, _) = call(addr, "GET", "/v1/nope".into(), vec![], None).await;
    assert_eq!(code, 401, "unknown routes under /v1 are gated too");
    let user = vec![("Authorization", "bearer user".to_string())];
    let (code, _, _) = call(
        addr,
        "GET",
        "/v1/db/default/head".into(),
        user.clone(),
        None,
    )
    .await;
    assert_eq!(code, 200, "a lower-case scheme is accepted");
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/databases".into(),
        user.clone(),
        Some(json!({"name": "orders"}).to_string()),
    )
    .await;
    assert_eq!((code, v["code"].as_str()), (403, Some("PermissionDenied")));
    let admin = vec![("Authorization", "Bearer admin".to_string())];
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/databases".into(),
        admin.clone(),
        Some(json!({"name": "orders"}).to_string()),
    )
    .await;
    assert_eq!(
        (code, v["name"].as_str(), v["head"].as_u64()),
        (201, Some("orders"), Some(0))
    );
    let (code, v, _) = call(addr, "GET", "/v1/databases".into(), admin.clone(), None).await;
    assert_eq!(code, 200);
    assert_eq!(v["databases"].as_array().unwrap().len(), 2);
    let (code, _, _) = call(
        addr,
        "POST",
        "/v1/databases".into(),
        admin.clone(),
        Some(json!({"name": "orders"}).to_string()),
    )
    .await;
    assert_eq!(code, 409);
    let (code, _, _) = call(
        addr,
        "POST",
        "/v1/databases".into(),
        admin.clone(),
        Some(json!({"name": "Bad Name"}).to_string()),
    )
    .await;
    assert_eq!(code, 400);
    let (code, _, _) = call(addr, "GET", "/v1/db/orders/head".into(), user.clone(), None).await;
    assert_eq!(code, 200);
    let (code, _, _) = call(
        addr,
        "DELETE",
        "/v1/databases/orders".into(),
        admin.clone(),
        None,
    )
    .await;
    assert_eq!(code, 204);
    let (code, _, _) = call(
        addr,
        "DELETE",
        "/v1/databases/orders".into(),
        admin.clone(),
        None,
    )
    .await;
    assert_eq!(code, 404);
    let (code, _, _) = call(
        addr,
        "DELETE",
        "/v1/databases/default".into(),
        admin.clone(),
        None,
    )
    .await;
    assert_eq!(code, 409);
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/retain".into(),
        admin.clone(),
        Some(json!({"floor": 0}).to_string()),
    )
    .await;
    assert_eq!((code, v["floor"].as_u64()), (200, Some(0)));
    let (code, _, text) = call(addr, "GET", "/openapi.json".into(), vec![], None).await;
    assert_eq!(code, 200);
    assert!(text.contains("\"/v1/db/{db}/commit\""));
    let (code, _, text) = call(addr, "GET", "/".into(), vec![], None).await;
    assert_eq!(code, 200);
    assert!(text.contains("/openapi.json"));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_streams_live_events_and_ends_when_the_database_is_dropped() {
    let mut s = Server::start_with(with_http).await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    s.client
        .create_database(chron_proto::v1::CreateDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    // HTTP/1.0 makes the body close-delimited, so lines arrive as sent.
    let mut reader = tokio::task::spawn_blocking(move || {
        let mut st = std::net::TcpStream::connect(addr).unwrap();
        st.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        st.write_all(b"GET /v1/db/orders/tail?from=0 HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut r = BufReader::new(st);
        let mut line = String::new();
        loop {
            line.clear();
            r.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            assert!(!line.is_empty(), "headers ended early");
        }
        r
    })
    .await
    .unwrap();
    let addr2 = s.running.as_ref().unwrap().local_addr;
    let mut orders = with_db(plain_channel(addr2).await, "orders");
    orders
        .commit(chron_proto::v1::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("live", &["t"])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap();
    let (data, mut reader) = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if let Some(d) = line.strip_prefix("data:") {
                return (d.trim().to_string(), reader);
            }
        }
    })
    .await
    .unwrap();
    let ev: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        (ev["position"].as_u64(), ev["payload"].as_str()),
        (Some(0), Some("live"))
    );
    s.client
        .drop_database(chron_proto::v1::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    let end = tokio::task::spawn_blocking(move || {
        let mut lines = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            lines.push(line.clone());
        }
        lines.join("")
    })
    .await
    .unwrap();
    assert!(
        end.contains("event: error") && end.contains("dropped"),
        "{end}"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_route_answers_and_metrics_count_them() {
    let s = Server::start_with(|o| {
        with_http(o);
        o.metrics_listen = Some("127.0.0.1:0".parse().unwrap());
    })
    .await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let (code, _, text) = call(addr, "GET", "/openapi.json".into(), vec![], None).await;
    assert_eq!(code, 200);
    let doc: Value = serde_json::from_str(&text).unwrap();
    // Every documented (method, path) is routed: anything but 404/405.
    // Bodies are empty, so writes answer 400; the delete names a
    // database that cannot be dropped (409); the tail is HTTP/1.0 and
    // ends on its own only when the database goes, so it is skipped.
    for (path, ops) in doc["paths"].as_object().unwrap() {
        for method in ops.as_object().unwrap().keys() {
            if path == "/v1/db/{db}/tail" {
                continue;
            }
            let method: &'static str = match method.as_str() {
                "get" => "GET",
                "post" => "POST",
                "delete" => "DELETE",
                other => panic!("{other}"),
            };
            let concrete = path
                .replace("{db}", "default")
                .replace("{uuid}", AGG)
                .replace("{tag}", "t")
                .replace("{name}", "default");
            let (code, _, _) = call(addr, method, concrete.clone(), vec![], None).await;
            assert!(code != 404 && code != 405, "{method} {concrete} -> {code}");
        }
    }
    let (code, _, _) = call(addr, "GET", "/v1/nope".into(), vec![], None).await;
    assert_eq!(code, 404);
    let maddr = s.running.as_ref().unwrap().metrics_addr.unwrap();
    let body = tokio::task::spawn_blocking(move || http(maddr, "GET", "/metrics", &[], None).2)
        .await
        .unwrap();
    assert!(
        body.contains("chron_http_requests_total{route=\"/v1/db/{db}/head\",status=\"200\"} 1"),
        "{body}"
    );
    assert!(body.contains("chron_http_requests_total{route=\"unmatched\",status=\"404\"} 1"));
    s.stop().await;

    let s = Server::start().await;
    assert!(
        s.running.as_ref().unwrap().http_addr.is_none(),
        "off by default"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn https_is_served_and_plaintext_refused() {
    let p = chrond::testing::pki();
    let tls = chrond::Tls {
        cert_pem: p.server_cert_pem.clone().into_bytes(),
        key_pem: p.server_key_pem.clone().into_bytes(),
        client_ca_pem: None,
    };
    let s = Server::start_with_no_client(move |o| {
        with_http(o);
        o.tls = Some(tls);
    })
    .await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let ca = p.ca_pem.clone();
    let code = tokio::task::spawn_blocking(move || {
        use tokio_rustls::rustls::pki_types::pem::PemObject;
        use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
        use tokio_rustls::rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
        let mut roots = RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca.as_bytes()) {
            roots.add(c.unwrap()).unwrap();
        }
        let cfg = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let conn = ClientConnection::new(
            std::sync::Arc::new(cfg),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let sock = std::net::TcpStream::connect(addr).unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut tls = StreamOwned::new(conn, sock);
        tls.write_all(b"GET /v1/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut raw = String::new();
        let _ = tls.read_to_string(&mut raw);
        raw.split_whitespace()
            .nth(1)
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0)
    })
    .await
    .unwrap();
    assert_eq!(code, 200);
    let (plain_code, _, _) = call(addr, "GET", "/v1/status".into(), vec![], None).await;
    assert_ne!(
        plain_code, 200,
        "plaintext on a TLS listener gets no HTTP answer"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_zero_is_the_maximum_and_quoted_numbers_are_expectations() {
    let s = Server::start_with(with_http).await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let body = serde_json::json!({"appends": [{"aggregate": AGG, "expect": "0",
        "events": [{"payload": "a"}, {"payload": "b"}]}]})
    .to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(body),
    )
    .await;
    assert_eq!(code, 200, "{v}");
    let body = serde_json::json!({"appends": [{"aggregate": AGG, "expect": "0",
        "events": [{"payload": "c"}]}]})
    .to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(body),
    )
    .await;
    assert_eq!(
        (code, v["reason"].as_str()),
        (409, Some("expectation")),
        "{v}"
    );
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?limit=0".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(v["events"].as_array().unwrap().len(), 2);
    assert_eq!(v["next"], 2, "where the next page would start");
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?from=2".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 200);
    assert!(v["events"].as_array().unwrap().is_empty() && v["next"].is_null());
    // A binary tag goes in base64, the path tag being `-`.
    let body = serde_json::json!({"appends": [{"aggregate": "free",
        "events": [{"payload": "bin", "tags_base64": ["/w=="]}]}]})
    .to_string();
    let (code, _, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(body),
    )
    .await;
    assert_eq!(code, 200);
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/tags/-/events?tag_base64=%2Fw%3D%3D".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["events"][0]["payload"], "bin");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gateway_takes_commit_id_and_answers_duplicate() {
    let s = Server::start_with(with_http).await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let hex = "0123456789abcdef0123456789abcdef";
    let hyphenated = "01234567-89ab-cdef-0123-456789abcdef";
    let txn = |id: &str| {
        json!({"commit_id": id, "appends": [{"aggregate": AGG, "expect": "any", "events": [{"payload": "a"}, {"payload": "b"}]}]}).to_string()
    };
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(txn(hex)),
    )
    .await;
    assert_eq!(
        (code, v["duplicate"].as_bool(), v["first"].as_u64()),
        (200, Some(false), Some(0))
    );
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(txn(hyphenated)),
    )
    .await;
    assert_eq!(
        (
            code,
            v["duplicate"].as_bool(),
            v["first"].as_u64(),
            v["next"].as_u64()
        ),
        (200, Some(true), Some(0), Some(2))
    );
    let (code, v, _) = call(
        addr,
        "GET",
        "/v1/db/default/events?from=0&limit=10".into(),
        vec![],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let events = v["events"].as_array().unwrap();
    assert_eq!(events.len(), 2, "nothing was written twice");
    assert_eq!(events[0]["commit_id"], hyphenated);
    assert!(events[1].get("commit_id").is_none());
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(txn("not-an-id")),
    )
    .await;
    assert_eq!((code, v["code"].as_str()), (400, Some("InvalidArgument")));
    let other = json!({"commit_id": hex, "appends": [{"aggregate": AGG, "expect": "any", "events": [{"payload": "a"}]}]}).to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/db/default/commit".into(),
        vec![],
        Some(other),
    )
    .await;
    assert_eq!((code, v["code"].as_str()), (400, Some("InvalidArgument")));
    assert!(
        v["error"].as_str().unwrap().contains("different shape"),
        "{v}"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_members_routes_answer() {
    let s = Server::start_with(|o| {
        with_http(o);
        o.token = Some("user".into());
        o.admin_token = Some("admin".into());
    })
    .await;
    let addr = s.running.as_ref().unwrap().http_addr.unwrap();
    let user = vec![("Authorization", "Bearer user".to_string())];
    let admin = vec![("Authorization", "Bearer admin".to_string())];
    let (code, v, _) = call(addr, "GET", "/v1/members".into(), user.clone(), None).await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["members"]["version"], 0, "no peers: no group");
    assert_eq!(v["member"], true);
    let body = json!({"add": "http://x:1"}).to_string();
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/members".into(),
        user.clone(),
        Some(body.clone()),
    )
    .await;
    assert_eq!((code, v["code"].as_str()), (403, Some("PermissionDenied")));
    let (code, v, _) = call(
        addr,
        "POST",
        "/v1/members".into(),
        admin.clone(),
        Some(body),
    )
    .await;
    assert_eq!(
        (code, v["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "no peers: {v}"
    );
    let both = json!({"add": "http://x:1", "remove": "http://y:1"}).to_string();
    let (code, _, _) = call(addr, "POST", "/v1/members".into(), admin, Some(both)).await;
    assert_eq!(code, 400);
    let (code, v, _) = call(addr, "GET", "/v1/status".into(), user, None).await;
    assert_eq!((code, v["member"].as_bool()), (200, Some(true)));
    s.stop().await;
}
