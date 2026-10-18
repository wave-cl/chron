use std::path::PathBuf;

use chron_client::{Client, ClientConfig, ClientError, Identity, TlsConfig};
use clap::Args;

use crate::output::Usage;

/// How to reach the daemon. Every flag is global.
#[derive(Args, Debug, Clone)]
pub struct Conn {
    /// The daemon's address; `https://` enables TLS.
    #[arg(
        long,
        global = true,
        env = "CHRON_ADDR",
        default_value = "http://127.0.0.1:4242"
    )]
    pub addr: String,
    /// PEM of a private CA to verify the daemon with (system roots otherwise).
    #[arg(long, global = true, env = "CHRON_TLS_CA")]
    pub tls_ca: Option<PathBuf>,
    /// PEM client certificate (mTLS); requires --tls-key.
    #[arg(long, global = true, env = "CHRON_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// PEM client key.
    #[arg(long, global = true, env = "CHRON_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// The name to verify the daemon's certificate against.
    #[arg(long, global = true, env = "CHRON_TLS_DOMAIN")]
    pub tls_domain: Option<String>,
    /// Bearer token the daemon requires.
    #[arg(long, global = true, env = "CHRON_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// Token for the admin commands (backup, apply, retain, promote,
    /// fence) when the daemon has a separate one.
    #[arg(long, global = true, env = "CHRON_ADMIN_TOKEN", hide_env_values = true)]
    pub admin_token: Option<String>,
    /// Largest message to accept from the daemon, in bytes (match the
    /// daemon's --max-message-bytes).
    #[arg(
        long,
        global = true,
        env = "CHRON_MAX_MESSAGE_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub max_message_bytes: usize,
}

/// The daemon could not be reached at all (exit code 2).
#[derive(Debug)]
pub struct Unreachable {
    pub addr: String,
    pub source: ClientError,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot reach chrond at {}: {}", self.addr, self.source)
    }
}

impl std::error::Error for Unreachable {}

/// A PEM file named on the command line; unreadable is a usage error.
fn read(path: &PathBuf, what: &str) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| Usage(format!("cannot read the {what} at {}: {e}", path.display())).into())
}

impl Conn {
    pub fn config(&self) -> anyhow::Result<ClientConfig> {
        let tls = if self.addr.starts_with("https://") {
            Some(TlsConfig {
                ca_pem: self.tls_ca.as_ref().map(|p| read(p, "CA")).transpose()?,
                identity: match (&self.tls_cert, &self.tls_key) {
                    (Some(c), Some(k)) => Some(Identity {
                        cert_pem: read(c, "client certificate")?,
                        key_pem: read(k, "client key")?,
                    }),
                    _ => None,
                },
                domain: self.tls_domain.clone(),
            })
        } else {
            None
        };
        Ok(ClientConfig {
            tls,
            token: self.token.clone(),
            admin_token: self.admin_token.clone(),
            max_message_bytes: self.max_message_bytes,
            ..ClientConfig::default()
        })
    }
}

pub async fn connect(conn: &Conn) -> anyhow::Result<Client> {
    let config = conn.config()?;
    Client::connect(conn.addr.clone(), config)
        .await
        .map_err(|source| {
            anyhow::Error::new(Unreachable {
                addr: conn.addr.clone(),
                source,
            })
        })
}
