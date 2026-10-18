use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use chron::{BackupMeta, Committed, Event, Expected, NewEvent, Retained, Uuid};
use chron_proto::convert;
use chron_proto::v1 as pb;
use chron_proto::v1::chron_client::ChronClient;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::error::ClientError;
use crate::txn::Transaction;

/// How to reach a daemon over TLS. Used only for `https://` addresses.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// A private CA; without it the system roots are used.
    pub ca_pem: Option<Vec<u8>>,
    /// A client certificate and key (mTLS).
    pub identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// The name to verify the server's certificate against, when it is not
    /// the address's host.
    pub domain: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub tls: Option<TlsConfig>,
    /// Sent as `authorization: Bearer <token>` on every call.
    pub token: Option<String>,
    /// Sent instead of `token` on the admin calls (promote, fence, retain,
    /// backup, apply, replicate) when the daemon requires a separate admin
    /// token; `None` sends `token` there too.
    pub admin_token: Option<String>,
    pub connect_timeout: Duration,
    /// Largest message accepted from the daemon; default 64 MiB.
    pub max_message_bytes: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            tls: None,
            token: None,
            admin_token: None,
            connect_timeout: Duration::from_secs(3),
            max_message_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The node's role as `Info` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Replica,
    Fenced,
    Unknown,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Leader => "leader",
            Role::Replica => "replica",
            Role::Fenced => "fenced",
            Role::Unknown => "unknown",
        })
    }
}

/// What a daemon reports about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub path: String,
    pub head: u64,
    pub floor: u64,
    pub segments: u64,
    pub bytes: u64,
    pub options: chron::Options,
    /// Whether the daemon's commits are synced to disk.
    pub fsync: bool,
    pub db_id: Option<Uuid>,
    pub epoch: u64,
    pub role: Role,
    pub replicating_from: Option<String>,
    pub replica_connected: bool,
    pub leader_head: Option<u64>,
    pub last_replication_error: Option<String>,
    pub fenced_by: Option<u64>,
}

/// Adds the bearer token, when there is one.
#[derive(Clone)]
pub struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, ClientError>> + Send + 'static>>;

type Inner = ChronClient<InterceptedService<Channel, Auth>>;

/// One replication chunk as the leader sent it.
#[derive(Debug, Clone)]
pub struct ReplicationChunk {
    /// The leader's database identity.
    pub db_id: Option<Uuid>,
    /// The records, ready for [`chron::Db::apply_chunk`].
    pub chunk: chron::Chunk,
    /// The leader's head when the chunk was cut.
    pub head: u64,
    /// The leader's epoch when the chunk was cut.
    pub epoch: u64,
}

/// A stream of replication chunks.
pub type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<ReplicationChunk, ClientError>> + Send + 'static>>;

/// A connection to one daemon. Cheap to clone; every method takes `&self`.
#[derive(Clone)]
pub struct Client {
    inner: Inner,
    admin: Inner,
    health: HealthClient<Channel>,
}

fn auth(token: Option<&str>) -> Result<Auth, ClientError> {
    Ok(Auth(match token {
        Some(t) => Some(format!("Bearer {t}").parse().map_err(|_| {
            ClientError::InvalidArgument("the token is not valid header text".into())
        })?),
        None => None,
    }))
}

impl Client {
    pub async fn connect(
        addr: impl Into<String>,
        config: ClientConfig,
    ) -> Result<Client, ClientError> {
        let addr = addr.into();
        let mut endpoint = Endpoint::from_shared(addr.clone())
            .map_err(|e| ClientError::InvalidArgument(format!("{addr}: {e}")))?
            .connect_timeout(config.connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(30)));
        if config.tls.is_some() && !addr.starts_with("https://") {
            return Err(ClientError::InvalidArgument(format!(
                "{addr} is not an https:// address, but TLS was configured"
            )));
        }
        if addr.starts_with("https://") {
            let tls = config.tls.clone().unwrap_or_default();
            let mut cfg = ClientTlsConfig::new();
            match &tls.ca_pem {
                Some(ca) => cfg = cfg.ca_certificate(Certificate::from_pem(ca)),
                None => cfg = cfg.with_native_roots(),
            }
            if let Some((cert, key)) = &tls.identity_pem {
                cfg = cfg.identity(Identity::from_pem(cert, key));
            }
            if let Some(d) = &tls.domain {
                cfg = cfg.domain_name(d.clone());
            }
            endpoint = endpoint.tls_config(cfg)?;
        }
        let channel = endpoint.connect().await?;
        let n = config.max_message_bytes;
        let inner = ChronClient::with_interceptor(channel.clone(), auth(config.token.as_deref())?)
            .max_decoding_message_size(n)
            .max_encoding_message_size(n);
        let admin = ChronClient::with_interceptor(
            channel.clone(),
            auth(config.admin_token.as_deref().or(config.token.as_deref()))?,
        )
        .max_decoding_message_size(n)
        .max_encoding_message_size(n);
        Ok(Client {
            inner,
            admin,
            health: HealthClient::new(channel),
        })
    }

    fn rpc(&self) -> Inner {
        self.inner.clone()
    }

    fn admin(&self) -> Inner {
        self.admin.clone()
    }

    async fn events(stream: tonic::Streaming<pb::Event>) -> Result<Vec<Event>, ClientError> {
        let mut stream = stream;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(convert::event_from_wire(item?)?);
        }
        Ok(out)
    }

    /// Commits every append atomically. [`ClientError::Conflict`] when an
    /// expectation fails; nothing is written then.
    pub async fn commit(&self, txn: Transaction) -> Result<Committed, ClientError> {
        self.commit_inner(txn, None).await
    }

    /// Like [`commit`](Client::commit), carrying the epoch the writer
    /// expects the daemon to be at. [`ClientError::Fenced`] when it is not.
    pub async fn commit_with_epoch(
        &self,
        txn: Transaction,
        epoch: u64,
    ) -> Result<Committed, ClientError> {
        self.commit_inner(txn, Some(epoch)).await
    }

    async fn commit_inner(
        &self,
        txn: Transaction,
        epoch: Option<u64>,
    ) -> Result<Committed, ClientError> {
        let appends = txn
            .into_appends()
            .into_iter()
            .map(|a| convert::append_to_wire(a.aggregate, a.expected, a.events))
            .collect();
        let resp = self
            .rpc()
            .commit(pb::CommitRequest { appends, epoch })
            .await?
            .into_inner();
        Ok(convert::committed_from_wire(resp)?)
    }

    /// One append in one transaction.
    pub async fn append(
        &self,
        aggregate: Uuid,
        expected: Expected,
        events: Vec<NewEvent>,
    ) -> Result<Committed, ClientError> {
        let mut txn = Transaction::new();
        txn.append(aggregate, expected, events);
        self.commit(txn).await
    }

    /// The aggregate's events from `from` (an index), at most `limit`
    /// (0 for all); backward, `from` is the inclusive upper bound and
    /// `u64::MAX` means from the end.
    pub async fn read_aggregate(
        &self,
        id: Uuid,
        from: u64,
        limit: u64,
        backward: bool,
    ) -> Result<Vec<Event>, ClientError> {
        let stream = self
            .rpc()
            .read_aggregate(pb::ReadAggregateRequest {
                aggregate: id.as_bytes().to_vec(),
                from_index: from,
                limit,
                backward,
            })
            .await?
            .into_inner();
        Self::events(stream).await
    }

    pub async fn read_all(
        &self,
        from: u64,
        limit: u64,
        backward: bool,
    ) -> Result<Vec<Event>, ClientError> {
        let stream = self
            .rpc()
            .read_all(pb::ReadAllRequest {
                from,
                limit,
                backward,
            })
            .await?
            .into_inner();
        Self::events(stream).await
    }

    pub async fn read_tag(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
        limit: u64,
        backward: bool,
    ) -> Result<Vec<Event>, ClientError> {
        let stream = self
            .rpc()
            .read_tag(pb::ReadTagRequest {
                tag: tag.into().to_vec(),
                from,
                limit,
                backward,
            })
            .await?
            .into_inner();
        Self::events(stream).await
    }

    pub async fn aggregate_len(&self, id: Uuid) -> Result<u64, ClientError> {
        Ok(self
            .rpc()
            .aggregate_len(pb::AggregateLenRequest {
                aggregate: id.as_bytes().to_vec(),
            })
            .await?
            .into_inner()
            .len)
    }

    pub async fn head(&self) -> Result<u64, ClientError> {
        Ok(self.rpc().head(pb::HeadRequest {}).await?.into_inner().head)
    }

    /// Committed events from `from` (or those carrying `tag`), then live
    /// ones. The stream never ends on its own.
    pub async fn subscribe(
        &self,
        from: u64,
        tag: Option<Bytes>,
    ) -> Result<EventStream, ClientError> {
        let stream = self
            .rpc()
            .subscribe(pb::SubscribeRequest {
                from,
                tag: tag.map(|t| t.to_vec()),
            })
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|item| {
            let ev = item?;
            Ok(convert::event_from_wire(ev)?)
        })))
    }

    pub async fn flush(&self) -> Result<(), ClientError> {
        self.rpc().flush(pb::FlushRequest {}).await?;
        Ok(())
    }

    pub async fn info(&self) -> Result<Info, ClientError> {
        let i = self.rpc().info(pb::InfoRequest {}).await?.into_inner();
        let role = match i.role() {
            pb::Role::Leader => Role::Leader,
            pb::Role::Replica => Role::Replica,
            pb::Role::Fenced => Role::Fenced,
            pb::Role::Unspecified => Role::Unknown,
        };
        Ok(Info {
            path: i.path,
            head: i.head,
            floor: i.floor,
            segments: i.segments,
            bytes: i.bytes,
            options: convert::options_from_wire(i.options.unwrap_or_default()),
            fsync: i.fsync,
            db_id: convert::aggregate(&i.db_id).ok(),
            epoch: i.epoch,
            role,
            replicating_from: i.replicating_from,
            replica_connected: i.replica_connected,
            leader_head: i.leader_head,
            last_replication_error: i.last_replication_error,
            fenced_by: i.fenced_by,
        })
    }

    /// A backup written by the daemon to `archive` on its own filesystem;
    /// incremental from `since` when given.
    pub async fn backup(
        &self,
        archive: &str,
        since: Option<u64>,
    ) -> Result<BackupMeta, ClientError> {
        let m = self
            .admin()
            .backup(pb::BackupRequest {
                archive: archive.to_string(),
                since,
            })
            .await?
            .into_inner();
        Ok(convert::backup_meta_from_wire(m))
    }

    pub async fn apply_backup(&self, archive: &str) -> Result<BackupMeta, ClientError> {
        let m = self
            .admin()
            .apply_backup(pb::ApplyBackupRequest {
                archive: archive.to_string(),
            })
            .await?
            .into_inner();
        Ok(convert::backup_meta_from_wire(m))
    }

    pub async fn retain(&self, floor: u64) -> Result<Retained, ClientError> {
        let r = self
            .admin()
            .retain(pb::RetainRequest { floor })
            .await?
            .into_inner();
        Ok(convert::retained_from_wire(r))
    }

    /// Makes a replica the leader; returns its new epoch and head.
    pub async fn promote(&self) -> Result<(u64, u64), ClientError> {
        let r = self
            .admin()
            .promote(pb::PromoteRequest {})
            .await?
            .into_inner();
        Ok((r.epoch, r.head))
    }

    /// Tells a leader that `epoch` exists; returns the daemon's own epoch.
    pub async fn fence(&self, epoch: u64) -> Result<u64, ClientError> {
        Ok(self
            .admin()
            .fence(pb::FenceRequest { epoch })
            .await?
            .into_inner()
            .epoch)
    }

    /// Whole transactions from `from` on, then live ones: what a replica
    /// pulls. `last_fingerprint` is the crc32 of the record before `from`
    /// as the caller holds it, so the daemon can refuse a diverged
    /// history. An admin call.
    pub async fn replicate(
        &self,
        from: u64,
        last_fingerprint: Option<u32>,
    ) -> Result<ChunkStream, ClientError> {
        let stream = self
            .admin()
            .replicate(pb::ReplicateRequest {
                from_position: from,
                last_fingerprint,
            })
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|item| {
            let c = item?;
            Ok(ReplicationChunk {
                db_id: Uuid::from_slice(&c.db_id).ok(),
                chunk: chron::Chunk {
                    from: c.from,
                    to: c.to,
                    frames: c.frames,
                },
                head: c.head,
                epoch: c.epoch,
            })
        })))
    }

    /// Whether `service` (`""` for the daemon as a whole, or
    /// `"chron.v1.Chron"`) reports SERVING. Needs no token.
    pub async fn health(&self, service: &str) -> Result<bool, ClientError> {
        let r = self
            .health
            .clone()
            .check(HealthCheckRequest {
                service: service.to_string(),
            })
            .await?
            .into_inner();
        Ok(r.status() == ServingStatus::Serving)
    }
}
