#![cfg(feature = "test-util")]

use std::time::Duration;

use chron_proto::v1 as pb;
use chron_proto::v1::chron_client::ChronClient;
use chrond::testing::{Pki, pki};
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::common::*;

async fn tls_channel(
    addr: std::net::SocketAddr,
    pki: &Pki,
    identity: Option<(&str, &str)>,
) -> Result<Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    if let Some((cert, key)) = identity {
        tls = tls.identity(Identity::from_pem(cert, key));
    }
    Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
}

fn tls_opts(p: &Pki, mtls: bool) -> chrond::Tls {
    chrond::Tls {
        cert_pem: p.server_cert_pem.clone().into_bytes(),
        key_pem: p.server_key_pem.clone().into_bytes(),
        client_ca_pem: mtls.then(|| p.ca_pem.clone().into_bytes()),
    }
}

async fn head_of<T>(client: &mut ChronClient<T>) -> Result<u64, tonic::Status>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    client
        .head(pb::HeadRequest {})
        .await
        .map(|r| r.into_inner().head)
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_serves_and_a_plaintext_client_is_refused() {
    let p = pki();
    let tls = tls_opts(&p, false);
    let s = Server::start_with_no_client(move |o| o.tls = Some(tls)).await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut secure = ChronClient::new(tls_channel(addr, &p, None).await.unwrap());
    secure
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), new(), vec![new_event("tls", &[])])],
            epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(head_of(&mut secure).await.unwrap(), 1);
    let plain = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await;
    let refused = match plain {
        Err(_) => true,
        Ok(ch) => head_of(&mut ChronClient::new(ch)).await.is_err(),
    };
    assert!(refused, "a plaintext client must not get through");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_requires_a_certificate_from_the_ca() {
    let p = pki();
    let tls = tls_opts(&p, true);
    let s = Server::start_with_no_client(move |o| o.tls = Some(tls)).await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut good = ChronClient::new(
        tls_channel(addr, &p, Some((&p.client_cert_pem, &p.client_key_pem)))
            .await
            .unwrap(),
    );
    assert_eq!(head_of(&mut good).await.unwrap(), 0);
    let none = match tls_channel(addr, &p, None).await {
        Err(_) => true,
        Ok(ch) => head_of(&mut ChronClient::new(ch)).await.is_err(),
    };
    assert!(none, "no client certificate must be refused");
    let other = match tls_channel(
        addr,
        &p,
        Some((&p.other_ca_client_cert_pem, &p.other_ca_client_key_pem)),
    )
    .await
    {
        Err(_) => true,
        Ok(ch) => head_of(&mut ChronClient::new(ch)).await.is_err(),
    };
    assert!(other, "a certificate from another CA must be refused");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn token_missing_or_wrong_is_unauthenticated() {
    let s = Server::start_with_no_client(|o| o.token = Some("s3cret".into())).await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let channel = plain_channel(addr).await;
    let mut bare = ChronClient::new(channel.clone());
    assert_eq!(
        head_of(&mut bare).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut wrong = with_token(channel.clone(), "nope");
    assert_eq!(
        head_of(&mut wrong).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let stream = bare
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await;
    assert_eq!(stream.unwrap_err().code(), Code::Unauthenticated);
    let mut right = with_token(channel.clone(), "s3cret");
    right
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), new(), vec![new_event("ok", &[])])],
            epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(head_of(&mut right).await.unwrap(), 1);
    let mut health = HealthClient::new(channel);
    let status = health
        .check(HealthCheckRequest {
            service: "chron.v1.Chron".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .status();
    assert_eq!(status, ServingStatus::Serving, "health needs no token");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn health_is_serving_then_not_serving_on_shutdown() {
    let mut s = Server::start().await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut health = HealthClient::new(plain_channel(addr).await);
    for service in ["", "chron.v1.Chron"] {
        let st = health
            .check(HealthCheckRequest {
                service: service.into(),
            })
            .await
            .unwrap()
            .into_inner()
            .status();
        assert_eq!(st, ServingStatus::Serving, "{service:?}");
    }
    // A `Watch` stream would hold the graceful shutdown open; probes poll.
    let running = s.running.take().unwrap();
    let stopped = tokio::spawn(running.shutdown());
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match health
            .check(HealthCheckRequest {
                service: "chron.v1.Chron".into(),
            })
            .await
        {
            Ok(r) if r.get_ref().status() == ServingStatus::NotServing => break,
            Ok(_) => {}
            Err(_) => break, // the connection ended with the server
        }
        assert!(std::time::Instant::now() < deadline, "still serving");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stopped.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_message_is_refused_and_nothing_written() {
    let mut s = Server::start_with(|o| {
        o.max_record_bytes = Some(512 * 1024);
        o.max_message_bytes = 1024 * 1024;
    })
    .await;
    let big = pb::NewEvent {
        tags: vec![],
        metadata: vec![],
        payload: vec![7u8; 2 * 1024 * 1024],
    };
    let err = s
        .client
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![big])],
            epoch: None,
        })
        .await
        .unwrap_err();
    eprintln!("oversized message: {:?}: {}", err.code(), err.message());
    assert!(
        err.message().contains("message length too large") || err.code() != Code::Ok,
        "{err}"
    );
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        0
    );
    fill(&mut s.client, agg(1), 1).await;
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        1
    );
    let start = chrond::start({
        let mut o =
            chrond::Options::new(s.dir.path().join("other"), "127.0.0.1:0".parse().unwrap());
        o.max_message_bytes = 1024;
        o
    })
    .await;
    assert!(
        start.is_err(),
        "a message limit below the record limit is refused"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn request_timeout_does_not_cut_streams() {
    let mut s = Server::start_with(|o| o.request_timeout = Some(Duration::from_millis(200))).await;
    let mut stream = s
        .client
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(500)).await;
    fill(&mut s.client, agg(1), 1).await;
    let ev = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.position, 0);
    s.stop().await;
}
