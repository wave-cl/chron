use std::time::Duration;

use anyhow::Context;
use chron_proto::v1::chron_client::ChronClient;
use tonic::transport::{Channel, Endpoint};

/// The daemon could not be reached at all (exit code 2).
#[derive(Debug)]
pub struct Unreachable {
    pub addr: String,
    pub source: tonic::transport::Error,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot reach chrond at {}: {}", self.addr, self.source)
    }
}

impl std::error::Error for Unreachable {}

pub async fn connect(addr: &str) -> anyhow::Result<ChronClient<Channel>> {
    let endpoint = Endpoint::from_shared(addr.to_string())
        .with_context(|| format!("{addr} is not a valid address"))?
        .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await.map_err(|source| {
        anyhow::Error::new(Unreachable {
            addr: addr.to_string(),
            source,
        })
    })?;
    Ok(ChronClient::new(channel))
}
