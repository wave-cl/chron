use std::path::PathBuf;

use chron_client::{Client, ClientConfig, ClientError, Consistency, Identity, TlsConfig};
use clap::Args;

use crate::output::Usage;

/// How to reach the daemon. Every flag is global.
#[derive(Args, Debug, Clone)]
pub struct Conn {
    /// The daemon's address; `https://` enables TLS. A comma list names a
    /// failover group: each is asked who leads and the leader is used.
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
    /// The database to address; the daemon's `default` when unset.
    #[arg(long = "db", global = true, env = "CHRON_DB")]
    pub db: Option<String>,
    /// Largest message to accept from the daemon, in bytes (match the
    /// daemon's --max-message-bytes).
    #[arg(
        long,
        global = true,
        env = "CHRON_MAX_MESSAGE_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub max_message_bytes: usize,
    /// Read everything the node holds, including what a leader's death
    /// could still erase (the daemon's default is confirmed-only when it
    /// waits for a majority on commits).
    #[arg(long, global = true, conflicts_with = "confirmed")]
    pub local: bool,
    /// Read only positions a majority of the group durably holds.
    #[arg(long, global = true)]
    pub confirmed: bool,
}

/// The daemon could not be reached at all (exit code 2).
#[derive(Debug)]
pub struct Unreachable {
    pub addr: String,
    pub source: ClientError,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            ClientError::NoLeader(notes) => write!(f, "no leader among {}: {notes}", self.addr),
            source => {
                write!(f, "cannot reach chrond at {}: {source}", self.addr)?;
                let mut cause = std::error::Error::source(source);
                while let Some(c) = cause {
                    write!(f, ": {c}")?;
                    cause = c.source();
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Unreachable {}

/// A PEM file named on the command line; unreadable is a usage error.
fn read(path: &PathBuf, what: &str) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| Usage(format!("cannot read the {what} at {}: {e}", path.display())).into())
}

impl Conn {
    /// The addresses `--addr` names (one, or a comma list).
    pub fn addrs(&self) -> Vec<String> {
        self.addr
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string)
            .collect()
    }

    pub fn config(&self) -> anyhow::Result<ClientConfig> {
        let addrs = self.addrs();
        if addrs.is_empty() {
            return Err(Usage("--addr names no address".into()).into());
        }
        let https = addrs.iter().filter(|a| a.starts_with("https://")).count();
        if https != 0 && https != addrs.len() {
            return Err(Usage(
                "--addr mixes http:// and https:// addresses; one scheme for the whole list".into(),
            )
            .into());
        }
        let tls = if https == addrs.len() {
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
            database: self.db.clone(),
            consistency: match (self.local, self.confirmed) {
                (true, _) => Consistency::Local,
                (_, true) => Consistency::Confirmed,
                _ => Consistency::Default,
            },
            ..ClientConfig::default()
        })
    }
}

pub async fn connect(conn: &Conn) -> anyhow::Result<Client> {
    let config = conn.config()?;
    let addrs = conn.addrs();
    let r = match addrs.as_slice() {
        [one] => Client::connect(one.clone(), config).await,
        _ => Client::connect_any(addrs, config).await,
    };
    r.map_err(|source| {
        anyhow::Error::new(Unreachable {
            addr: conn.addr.clone(),
            source,
        })
    })
}

/// A plain connection to one address of the list, for commands that ask
/// every node (`discover`).
pub async fn connect_one(conn: &Conn, addr: &str) -> anyhow::Result<Client> {
    Client::connect(addr.to_string(), conn.config()?)
        .await
        .map_err(|source| {
            anyhow::Error::new(Unreachable {
                addr: addr.to_string(),
                source,
            })
        })
}
