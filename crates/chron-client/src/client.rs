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
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::error::ClientError;
use crate::txn::Transaction;

/// A client certificate and its key, both PEM (mTLS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The certificate chain.
    pub cert_pem: Vec<u8>,
    /// The private key.
    pub key_pem: Vec<u8>,
}

/// How to reach a daemon over TLS. Only an `https://` address uses it;
/// configuring it for another address is an error at connect.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// A private CA; without it the system roots are used.
    pub ca_pem: Option<Vec<u8>>,
    /// A certificate to present to the daemon (mTLS).
    pub identity: Option<Identity>,
    /// The name to verify the server's certificate against, when it is not
    /// the address's host.
    pub domain: Option<String>,
}

/// How to connect: transport, credentials and limits.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// TLS settings for an `https://` address.
    pub tls: Option<TlsConfig>,
    /// Sent as `authorization: Bearer <token>` on every call.
    pub token: Option<String>,
    /// Sent instead of `token` on the admin calls (promote, fence, retain,
    /// backup, apply, replicate) when the daemon requires a separate admin
    /// token; `None` sends `token` there too.
    pub admin_token: Option<String>,
    /// Bound on establishing the connection.
    pub connect_timeout: Duration,
    /// Bound on each call's response head; streams keep running after it.
    /// `None` (the default) waits as long as the daemon takes.
    pub request_timeout: Option<Duration>,
    /// Largest message accepted from, or sent to, the daemon; default
    /// 64 MiB, matching the daemon's default.
    pub max_message_bytes: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            tls: None,
            token: None,
            admin_token: None,
            connect_timeout: Duration::from_secs(3),
            request_timeout: None,
            max_message_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The node's role as `Info` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Accepts writes.
    Leader,
    /// Follows a leader; read-only.
    Replica,
    /// A newer leader exists; refuses writes until restarted as a replica.
    Fenced,
    /// A role this client does not know (a newer daemon).
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
#[non_exhaustive]
pub struct Info {
    /// The database directory on the daemon's filesystem.
    pub path: String,
    /// One past the last committed position.
    pub head: u64,
    /// The first readable position (retention moves it up).
    pub floor: u64,
    /// Segment files on disk.
    pub segments: u64,
    /// Bytes in those segments.
    pub bytes: u64,
    /// The options the daemon opened the database with.
    pub options: chron::Options,
    /// Whether the daemon's commits are synced to disk.
    pub fsync: bool,
    /// The database's identity, shared by its backups and replicas.
    pub db_id: Uuid,
    /// The database's epoch; rises on every promotion.
    pub epoch: u64,
    /// Leader, replica or fenced.
    pub role: Role,
    /// The leader's address, for a replica.
    pub replicating_from: Option<String>,
    /// Whether a replica currently holds a stream from its leader.
    pub replica_connected: bool,
    /// The leader's head as a replica last saw it.
    pub leader_head: Option<u64>,
    /// Why a replica is not connected, when it is not.
    pub last_replication_error: Option<String>,
    /// The epoch that fenced this node, for a fenced one.
    pub fenced_by: Option<u64>,
}

/// What `promote` returns: the new leader's epoch and head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Promoted {
    /// The epoch the node now leads under.
    pub epoch: u64,
    /// Its head at promotion.
    pub head: u64,
}

/// Adds the bearer token, when there is one.
#[derive(Clone)]
struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

/// Events as the daemon sends them. A finite read's stream ends at the
/// read's end; a subscription's ends only with an error (the daemon
/// fenced or stopped).
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

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Client")
    }
}

fn auth(token: Option<&str>) -> Result<Auth, ClientError> {
    Ok(Auth(match token {
        Some(t) => Some(format!("Bearer {t}").parse().map_err(|_| {
            ClientError::InvalidArgument("the token is not valid header text".into())
        })?),
        None => None,
    }))
}

/// One finite read: what, from where, how many, which way.
enum Read {
    Aggregate(Uuid),
    All,
    Tag(Bytes),
}

fn events(stream: tonic::Streaming<pb::Event>) -> EventStream {
    Box::pin(stream.map(|item| Ok(convert::event_from_wire(item?)?)))
}

impl Client {
    /// Connects to `addr` (`http://host:port` or `https://host:port`).
    /// Keepalive pings keep an idle connection alive. Connecting does not
    /// authenticate: a wrong token shows on the first call.
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
        if let Some(t) = config.request_timeout {
            endpoint = endpoint.timeout(t);
        }
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
            if let Some(id) = &tls.identity {
                cfg = cfg.identity(tonic::transport::Identity::from_pem(
                    &id.cert_pem,
                    &id.key_pem,
                ));
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
        let appends = txn.into_appends();
        let expectations: Vec<(Option<Uuid>, Expected)> =
            appends.iter().map(|a| (a.aggregate, a.expected)).collect();
        let wire = appends
            .into_iter()
            .map(|a| convert::append_to_wire(a.aggregate, a.expected, a.events))
            .collect();
        let resp = self
            .rpc()
            .commit(pb::CommitRequest {
                appends: wire,
                epoch,
            })
            .await
            .map_err(|s| match ClientError::from(s) {
                ClientError::Conflict {
                    aggregate,
                    actual,
                    message,
                    ..
                } => ClientError::Conflict {
                    aggregate,
                    actual,
                    expected: expectations
                        .iter()
                        .find(|(a, _)| *a == Some(aggregate))
                        .map(|(_, e)| *e),
                    message,
                },
                e => e,
            })?
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

    async fn read(
        &self,
        read: Read,
        from: u64,
        limit: usize,
        backward: bool,
    ) -> Result<EventStream, ClientError> {
        let limit = limit as u64;
        let stream = match read {
            Read::Aggregate(id) => self
                .rpc()
                .read_aggregate(pb::ReadAggregateRequest {
                    aggregate: id.as_bytes().to_vec(),
                    from_index: from,
                    limit,
                    backward,
                })
                .await?
                .into_inner(),
            Read::All => self
                .rpc()
                .read_all(pb::ReadAllRequest {
                    from,
                    limit,
                    backward,
                })
                .await?
                .into_inner(),
            Read::Tag(tag) => self
                .rpc()
                .read_tag(pb::ReadTagRequest {
                    tag: tag.to_vec(),
                    from,
                    limit,
                    backward,
                })
                .await?
                .into_inner(),
        };
        Ok(events(stream))
    }

    async fn collect(stream: EventStream) -> Result<Vec<Event>, ClientError> {
        let mut stream = stream;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// The aggregate's events from index `from_index` on, at most `limit`
    /// (0 for all). An unknown aggregate reads as empty.
    pub async fn read_aggregate(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_aggregate_stream(id, from_index, limit).await?).await
    }

    /// Like [`read_aggregate`](Client::read_aggregate), newest first from
    /// index `to_index` (inclusive; `u64::MAX` for the end).
    pub async fn read_aggregate_backward(
        &self,
        id: Uuid,
        to_index: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(
            self.read_aggregate_backward_stream(id, to_index, limit)
                .await?,
        )
        .await
    }

    /// Every event from position `from` on, at most `limit` (0 for all).
    /// [`ClientError::PositionOutOfRange`] past the head,
    /// [`ClientError::BelowFloor`] below the floor.
    pub async fn read_all(&self, from: u64, limit: usize) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_all_stream(from, limit).await?).await
    }

    /// Like [`read_all`](Client::read_all), newest first from position
    /// `to` (inclusive; `u64::MAX` for the end).
    pub async fn read_all_backward(
        &self,
        to: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_all_backward_stream(to, limit).await?).await
    }

    /// The events carrying `tag` from position `from` on, at most `limit`
    /// (0 for all). Below the floor, from the floor.
    pub async fn read_tag(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_tag_stream(tag, from, limit).await?).await
    }

    /// Like [`read_tag`](Client::read_tag), newest first from position
    /// `to` (inclusive; `u64::MAX` for the end).
    pub async fn read_tag_backward(
        &self,
        tag: impl Into<Bytes>,
        to: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_tag_backward_stream(tag, to, limit).await?).await
    }

    /// [`read_aggregate`](Client::read_aggregate) as a stream, for reads
    /// too large to hold.
    pub async fn read_aggregate_stream(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::Aggregate(id), from_index, limit, false)
            .await
    }

    /// [`read_aggregate_backward`](Client::read_aggregate_backward) as a
    /// stream.
    pub async fn read_aggregate_backward_stream(
        &self,
        id: Uuid,
        to_index: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::Aggregate(id), to_index, limit, true).await
    }

    /// [`read_all`](Client::read_all) as a stream. A bad `from` fails the
    /// call, not the stream.
    pub async fn read_all_stream(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::All, from, limit, false).await
    }

    /// [`read_all_backward`](Client::read_all_backward) as a stream.
    pub async fn read_all_backward_stream(
        &self,
        to: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::All, to, limit, true).await
    }

    /// [`read_tag`](Client::read_tag) as a stream.
    pub async fn read_tag_stream(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::Tag(tag.into()), from, limit, false).await
    }

    /// [`read_tag_backward`](Client::read_tag_backward) as a stream.
    pub async fn read_tag_backward_stream(
        &self,
        tag: impl Into<Bytes>,
        to: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::Tag(tag.into()), to, limit, true).await
    }

    /// How many events the aggregate has (0 for an unknown one).
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

    /// One past the last committed position.
    pub async fn head(&self) -> Result<u64, ClientError> {
        Ok(self.rpc().head(pb::HeadRequest {}).await?.into_inner().head)
    }

    /// Committed events from position `from` on, then live ones as they
    /// commit. The stream ends only with an error: [`ClientError::
    /// BelowFloor`] when `from` is below the floor, [`ClientError::
    /// Unavailable`] when the daemon is fenced or stops.
    pub async fn subscribe(&self, from: u64) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, None).await
    }

    /// Like [`subscribe`](Client::subscribe), only the events carrying
    /// `tag`; below the floor it starts at the floor.
    pub async fn subscribe_tag(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
    ) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, Some(tag.into())).await
    }

    async fn subscribe_inner(
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
        Ok(events(stream))
    }

    /// Syncs everything committed so far to disk.
    pub async fn flush(&self) -> Result<(), ClientError> {
        self.rpc().flush(pb::FlushRequest {}).await?;
        Ok(())
    }

    /// What the daemon reports about itself.
    pub async fn info(&self) -> Result<Info, ClientError> {
        let i = self.rpc().info(pb::InfoRequest {}).await?.into_inner();
        let role = match i.role() {
            pb::Role::Leader => Role::Leader,
            pb::Role::Replica => Role::Replica,
            pb::Role::Fenced => Role::Fenced,
            pb::Role::Unspecified => Role::Unknown,
        };
        let db_id = Uuid::from_slice(&i.db_id)
            .map_err(|_| ClientError::Decode("Info carries no database identity".into()))?;
        Ok(Info {
            path: i.path,
            head: i.head,
            floor: i.floor,
            segments: i.segments,
            bytes: i.bytes,
            options: convert::options_from_wire(i.options.unwrap_or_default()),
            fsync: i.fsync,
            db_id,
            epoch: i.epoch,
            role,
            replicating_from: i.replicating_from,
            replica_connected: i.replica_connected,
            leader_head: i.leader_head,
            last_replication_error: i.last_replication_error,
            fenced_by: i.fenced_by,
        })
    }

    /// A backup written by the daemon as the bare file name `archive`
    /// under its backup directory; incremental from `since` when given.
    /// An admin call.
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

    /// Applies the incremental backup named `archive` under the daemon's
    /// backup directory. An admin call.
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

    /// Drops whole segments below `floor`. An admin call.
    pub async fn retain(&self, floor: u64) -> Result<Retained, ClientError> {
        let r = self
            .admin()
            .retain(pb::RetainRequest { floor })
            .await?
            .into_inner();
        Ok(convert::retained_from_wire(r))
    }

    /// Makes a replica the leader. An admin call.
    pub async fn promote(&self) -> Result<Promoted, ClientError> {
        let r = self
            .admin()
            .promote(pb::PromoteRequest {})
            .await?
            .into_inner();
        Ok(Promoted {
            epoch: r.epoch,
            head: r.head,
        })
    }

    /// Tells a leader that `epoch` exists; returns the daemon's own epoch.
    /// An admin call.
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

    /// Whether `service` reports SERVING: `""` for the daemon as a whole,
    /// `"chron.v1.Chron"` for the service, `"chron.v1.Chron.leader"` only
    /// while the node leads. Needs no token.
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
