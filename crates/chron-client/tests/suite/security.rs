use std::time::Duration;

use chron_client::{Client, ClientConfig, ClientError, Expected, Identity, NewEvent, TlsConfig};
use chrond::testing::pki;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn token_config_sets_the_header() {
    let f = Fixture::start_with(
        |o| o.token = Some("s3cret".into()),
        ClientConfig {
            token: Some("s3cret".into()),
            ..ClientConfig::default()
        },
    )
    .await;
    f.client
        .append(agg(1), Expected::New, vec![NewEvent::new("ok")])
        .await
        .unwrap();
    let bare = Client::connect(format!("http://{}", f.addr()), ClientConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        bare.head().await,
        Err(ClientError::Unauthenticated(_))
    ));
    assert!(
        bare.health("chron.v1.Chron").await.unwrap(),
        "health needs no token"
    );
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_and_mtls_config() {
    let p = pki();
    let tls = chrond::Tls {
        cert_pem: p.server_cert_pem.clone().into_bytes(),
        key_pem: p.server_key_pem.clone().into_bytes(),
        client_ca_pem: Some(p.ca_pem.clone().into_bytes()),
    };
    let f = Fixture::start_with(
        move |o| o.tls = Some(tls),
        ClientConfig {
            tls: Some(TlsConfig {
                ca_pem: Some(p.ca_pem.clone().into_bytes()),
                identity: Some(Identity {
                    cert_pem: p.client_cert_pem.clone().into_bytes(),
                    key_pem: p.client_key_pem.clone().into_bytes(),
                }),
                domain: Some("localhost".into()),
            }),
            ..ClientConfig::default()
        },
    )
    .await;
    assert_eq!(f.client.head().await.unwrap(), 0);
    let without_identity = Client::connect(
        format!("https://{}", f.addr()),
        ClientConfig {
            tls: Some(TlsConfig {
                ca_pem: Some(p.ca_pem.clone().into_bytes()),
                identity: None,
                domain: Some("localhost".into()),
            }),
            ..ClientConfig::default()
        },
    )
    .await;
    let refused = match without_identity {
        Err(_) => true,
        Ok(c) => c.head().await.is_err(),
    };
    assert!(refused);
    let plain = Client::connect(format!("http://{}", f.addr()), ClientConfig::default()).await;
    let refused = match plain {
        Err(_) => true,
        Ok(c) => c.head().await.is_err(),
    };
    assert!(refused);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_config_on_a_plain_address_is_refused_before_connecting() {
    let err = Client::connect(
        "http://127.0.0.1:1",
        ClientConfig {
            tls: Some(TlsConfig::default()),
            ..ClientConfig::default()
        },
    )
    .await
    .expect_err("refused");
    assert!(matches!(err, ClientError::InvalidArgument(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_token_goes_on_admin_calls_only() {
    let f = Fixture::start_with(
        |o| {
            o.token = Some("user".into());
            o.admin_token = Some("admin".into());
        },
        ClientConfig {
            token: Some("user".into()),
            admin_token: Some("admin".into()),
            request_timeout: Some(Duration::from_secs(30)),
            ..ClientConfig::default()
        },
    )
    .await;
    f.client
        .append(agg(1), Expected::New, vec![NewEvent::new("ok")])
        .await
        .unwrap();
    f.client.retain(0).await.unwrap();
    let user_only = Client::connect(
        format!("http://{}", f.addr()),
        ClientConfig {
            token: Some("user".into()),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(user_only.head().await.unwrap(), 1);
    assert!(matches!(
        user_only.retain(0).await,
        Err(ClientError::PermissionDenied(_))
    ));
    f.stop().await;
}
