#![allow(dead_code)]

use chron_client::{Client, ClientConfig};

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub running: Option<chrond::Running>,
    pub client: Client,
}

impl Fixture {
    pub async fn start() -> Fixture {
        Self::start_with(|_| {}, ClientConfig::default()).await
    }

    pub async fn start_with(f: impl FnOnce(&mut chrond::Options), config: ClientConfig) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut opts =
            chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
        opts.fsync = false;
        f(&mut opts);
        let running = chrond::start(opts).await.expect("the daemon starts");
        let scheme = if running.node.db.path().exists() && config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let client = Client::connect(format!("{scheme}://{}", running.local_addr), config)
            .await
            .expect("connect");
        Fixture {
            dir,
            running: Some(running),
            client,
        }
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.running.as_ref().unwrap().local_addr
    }

    pub async fn stop(mut self) {
        self.running.take().unwrap().shutdown().await.unwrap();
    }
}

pub fn agg(n: u128) -> chron::Uuid {
    chron::Uuid::from_u128(n)
}

pub fn payloads(events: &[chron::Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
        .collect()
}
