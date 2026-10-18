use chron_client::{Client, ClientConfig, ClientError, Expected, NewEvent, TlsConfig};
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
                identity_pem: Some((
                    p.client_cert_pem.clone().into_bytes(),
                    p.client_key_pem.clone().into_bytes(),
                )),
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
                identity_pem: None,
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
