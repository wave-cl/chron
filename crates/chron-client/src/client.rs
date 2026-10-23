use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

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
    /// The database every call goes to (`chron-db` metadata); `None` is
    /// the daemon's `default`. [`Client::database`] rebinds a client.
    pub database: Option<String>,
    /// For a client made by [`Client::connect_any`]: how long a commit
    /// keeps looking for the leader and retrying after the node refused
    /// it as not the leader, or could not be reached. An election takes
    /// one to two lease ttls (10 s each by default), so the default is
    /// 30 s. See [`Client::commit`] for which failures are retried.
    pub failover_timeout: Duration,
    /// Whether commits wait for a majority of a failover group (`Some(
    /// true)`), return once on the leader's disk (`Some(false)`), or
    /// follow the daemon's `--sync-commit` (`None`, the default). A
    /// daemon without peers ignores it.
    pub sync_commit: Option<bool>,
    /// Give every commit a random commit id when the caller sets none
    /// (the default), so a failed commit can be sent again: the daemon
    /// answers a resend with the first landing instead of writing twice.
    /// See [`Client::commit`] for the retry rule it unlocks. Turn it off
    /// against 0.4.0 daemons, which drop the id and would write twice.
    pub commit_ids: bool,
    /// What reads, head and subscriptions see when a call does not say
    /// ([`ReadOptions::consistency`]): [`Consistency::Default`] (the
    /// daemon's own default) unless set.
    pub consistency: Consistency,
}

/// What a read, head or subscription may see.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Consistency {
    /// The daemon's default: confirmed when it waits for a majority on
    /// commits, local otherwise.
    #[default]
    Default,
    /// Everything the node holds, including what a leader's death could
    /// still erase.
    Local,
    /// Only positions a majority of the failover group durably holds;
    /// the same as `Local` on a node without peers.
    Confirmed,
}

impl Consistency {
    fn wire(self) -> pb::Consistency {
        match self {
            Consistency::Default => pb::Consistency::Unspecified,
            Consistency::Local => pb::Consistency::Local,
            Consistency::Confirmed => pb::Consistency::Confirmed,
        }
    }

    fn from_wire(c: i32) -> Consistency {
        match pb::Consistency::try_from(c).unwrap_or(pb::Consistency::Unspecified) {
            pb::Consistency::Unspecified => Consistency::Default,
            pb::Consistency::Local => Consistency::Local,
            pb::Consistency::Confirmed => Consistency::Confirmed,
        }
    }
}

/// Per-read choices (the `_with` reads and [`Client::head_with`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// What the read may see; `None` is the client's
    /// [`ClientConfig::consistency`].
    pub consistency: Option<Consistency>,
    /// Wait until the bound under that consistency reaches this position
    /// before reading (read your writes: a commit's [`Committed::next`]).
    pub at_least: Option<u64>,
    /// How long `at_least` may wait; `None` is the daemon's sync timeout,
    /// zero does not wait. Past it the read fails with
    /// [`ClientError::BoundTimeout`].
    pub wait_timeout: Option<Duration>,
}

/// A durable cursor's handle for one subscription (see
/// [`Client::subscribe_cursor`]).
#[derive(Debug, Clone)]
pub struct Cursor {
    client: Client,
    name: String,
    session: u64,
    /// Where this subscription started.
    pub from: u64,
}

impl Cursor {
    /// The cursor's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The session this subscription owns the cursor under.
    pub fn session(&self) -> u64 {
        self.session
    }

    /// Acknowledges everything below `next` (the position after the last
    /// event processed). Answers the stored position: `next` when it
    /// moved, the earlier one when `next` was not past it. Past the
    /// confirmed bound it is [`ClientError::InvalidArgument`]; from a
    /// session that was taken over, [`ClientError::TakenOver`].
    pub async fn ack(&self, next: u64) -> Result<u64, ClientError> {
        Ok(self
            .client
            .rpc()
            .ack(pb::AckRequest {
                cursor: self.name.clone(),
                next,
                session: self.session,
            })
            .await?
            .into_inner()
            .next)
    }
}

/// One durable cursor as [`Client::cursors`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorInfo {
    /// The cursor's name.
    pub name: String,
    /// The position after the last acknowledged event.
    pub next: u64,
    /// The session that owns it.
    pub session: u64,
}

/// A stream as [`Client::streams`] lists it: a named subset of the
/// global order, the events carrying any tag of a set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    /// The stream's name.
    pub name: String,
    /// The tags it covers, sorted, without duplicates.
    pub tags: Vec<Bytes>,
    /// Whether the fill at creation finished: until then the stream
    /// cannot be read.
    pub ready: bool,
}

/// What a subscription delivers: everything, one tag's events, or a
/// stream's members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// Every event.
    All,
    /// The events carrying the tag.
    Tag(Bytes),
    /// The members of the stream.
    Stream(String),
}

/// [`Client::cursors`]: the cursors and the confirmed bound (a cursor's
/// lag is `confirmed - next`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorList {
    /// Every cursor, by name.
    pub cursors: Vec<CursorInfo>,
    /// The confirmed bound.
    pub confirmed: u64,
}

/// What [`Client::head_with`] answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    /// The bound under the consistency asked for.
    pub head: u64,
    /// Positions a majority durably holds (the local head without peers).
    pub confirmed: u64,
    /// Everything the node holds.
    pub local: u64,
}

/// Per-commit choices.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitOptions {
    /// The epoch the daemon must be at (a fencing token).
    pub epoch: Option<u64>,
    /// Wait for a majority, or not; `None` is the client's
    /// [`ClientConfig::sync_commit`], then the daemon's default.
    pub sync: Option<bool>,
    /// The transaction's commit id; `None` lets the client choose one
    /// when [`ClientConfig::commit_ids`] is on. A commit resent with the
    /// same id and the same events' aggregates is answered with the
    /// first landing ([`Committed::duplicate`]), nothing written.
    pub commit_id: Option<[u8; 16]>,
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
            database: None,
            failover_timeout: Duration::from_secs(30),
            sync_commit: None,
            commit_ids: true,
            consistency: Consistency::Default,
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
    /// A newer leader exists; refuses writes until it follows that
    /// leader (with peers, by itself) or is restarted as a replica.
    Fenced,
    /// With peers: no leader known and no lease held; refuses writes until
    /// one is heard or elected.
    Candidate,
    /// With peers: not in the group's membership (removed, or not yet
    /// added). Serves reads, follows the leader without voting, refuses
    /// writes.
    Removed,
    /// A role this client does not know (a newer daemon).
    Unknown,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Leader => "leader",
            Role::Replica => "replica",
            Role::Fenced => "fenced",
            Role::Candidate => "candidate",
            Role::Removed => "removed",
            Role::Unknown => "unknown",
        })
    }
}

impl Role {
    fn from_wire(r: pb::Role) -> Role {
        match r {
            pb::Role::Leader => Role::Leader,
            pb::Role::Replica => Role::Replica,
            pb::Role::Fenced => Role::Fenced,
            pb::Role::Candidate => Role::Candidate,
            pb::Role::Removed => Role::Removed,
            pb::Role::Unspecified => Role::Unknown,
        }
    }
}

/// The failover group's membership as a daemon holds it: every member's
/// advertised address, stamped with the epoch of the leader that
/// published it and a version that rises by one per change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Members {
    /// The epoch of the leader that published this generation.
    pub epoch: u64,
    /// Rises by one per change; generations order by `(epoch, version)`.
    pub version: u64,
    /// Sorted, the daemon's own address included when it is a member.
    pub addrs: Vec<String>,
}

impl Members {
    fn from_wire(m: Option<pb::Members>) -> Members {
        let m = m.unwrap_or_default();
        let mut addrs = m.addrs;
        addrs.sort();
        Members {
            epoch: m.epoch,
            version: m.version,
            addrs,
        }
    }
}

/// What a daemon answers to `members`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MembersStatus {
    /// The membership the daemon holds.
    pub members: Members,
    /// On a leader: whether the generation is committed (a majority of
    /// it holds it and everything confirmed before it was made).
    pub committed: bool,
    /// Whether the daemon is in the membership.
    pub member: bool,
    /// The leader as the daemon knows it.
    pub leader_addr: Option<String>,
}

/// What a daemon answers to `discover`: who leads, as it knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Discovered {
    /// The leader's address: the daemon's own when it leads with a live
    /// lease, the one it follows, or the last one it heard.
    pub leader_addr: Option<String>,
    /// The daemon's epoch.
    pub epoch: u64,
    /// The daemon's role.
    pub role: Role,
    /// The other members of the daemon's membership (empty without
    /// failover).
    pub peers: Vec<String>,
    /// On a leader with peers: how much of its lease is left.
    pub lease_remaining: Option<Duration>,
    /// The membership generation the daemon holds (`(epoch, version)`).
    pub members_version: u64,
    /// The epoch half of that generation.
    pub members_epoch: u64,
}

/// What a daemon reports about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Info {
    /// The database the call was addressed to.
    pub database: String,
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
    /// The leader's address as the daemon knows it: its own on a leader
    /// with a live lease, the one heard within the lease otherwise.
    pub leader_addr: Option<String>,
    /// On a leader with peers: how much of its majority lease is left
    /// (zero once it expired and writes are refused).
    pub lease_remaining: Option<Duration>,
    /// The daemon's peers (empty without failover).
    pub peers: Vec<String>,
    /// The epoch of the vote the daemon last gave.
    pub voted_epoch: Option<u64>,
    /// On a leader with peers: the highest position a majority durably
    /// holds.
    pub majority_applied: Option<u64>,
    /// On a leader with peers: the followers attached through `Follow`.
    pub followers: Vec<FollowerStatus>,
    /// Whether the daemon waits for a majority by default.
    pub sync_commit: bool,
    /// On a node with peers: the confirmed bound (positions a majority
    /// durably holds, as this node knows it, clamped to its head).
    pub confirmed: Option<u64>,
    /// What reads get when they do not ask (never `Default`).
    pub read_consistency: Consistency,
    /// Streams defined on the database, ready or not.
    pub streams: u64,
    /// The membership generation the daemon holds (`(epoch, version)`).
    pub members_version: u64,
    /// The epoch half of that generation.
    pub members_epoch: u64,
    /// On a leader: whether that generation is committed.
    pub members_committed: bool,
    /// Whether the daemon is in the membership.
    pub member: bool,
}

/// One follower as the leader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FollowerStatus {
    /// Its advertised address.
    pub peer: String,
    /// The position it last reported applied.
    pub applied: u64,
    /// Whether it syncs what it applies to disk.
    pub durable: bool,
    /// Whether its acknowledgments count toward the majority.
    pub counted: bool,
}

/// The acknowledgment side of a [`Client::follow`] stream.
#[derive(Clone)]
pub struct FollowAcks(tokio::sync::mpsc::Sender<pb::FollowRequest>);

impl FollowAcks {
    /// Reports that everything below `head` is applied (durably, when
    /// the stream was opened as durable). Fails once the leader stopped
    /// reading.
    pub async fn applied(&self, head: u64) -> Result<(), ClientError> {
        self.0
            .send(pb::FollowRequest {
                msg: Some(pb::follow_request::Msg::Applied(pb::FollowApplied { head })),
            })
            .await
            .map_err(|_| {
                ClientError::Unavailable("the leader stopped reading acknowledgments".into())
            })
    }
}

/// What `promote` returns: the new leader's epoch and head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Promoted {
    /// The epoch the node now leads under.
    pub epoch: u64,
    /// Its head at promotion.
    pub head: u64,
}

/// One of the daemon's databases, as `list_databases` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatabaseInfo {
    /// Its name.
    pub name: String,
    /// One past its last committed position.
    pub head: u64,
    /// Its first readable position.
    pub floor: u64,
    /// Its epoch.
    pub epoch: u64,
    /// Its identity.
    pub db_id: Uuid,
    /// Whether it fsyncs (`None` from a daemon before 0.6.0).
    pub fsync: Option<bool>,
    /// Its record limit (`None` from a daemon before 0.6.0).
    pub max_record_bytes: Option<u64>,
}

/// A database's own choices at creation (see
/// [`Client::create_database_with`]); what is left `None` follows the
/// daemon's flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatabaseOptions {
    /// Sync every batch to disk before acknowledging it.
    pub fsync: Option<bool>,
    /// The largest record it accepts.
    pub max_record_bytes: Option<u64>,
}

/// A database dropped on the leader (see [`Client::list_databases_full`]):
/// replicas remove their copy by identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// The dropped database's identity.
    pub db_id: Uuid,
    /// Its name at the drop.
    pub name: String,
    /// When it was dropped, unix seconds.
    pub dropped_at: u64,
}

/// [`Client::list_databases_full`]: the databases and the recent drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// Every database.
    pub databases: Vec<DatabaseInfo>,
    /// Databases dropped within the last 30 days.
    pub dropped: Vec<Tombstone>,
}

/// Adds the bearer token and the database name, when set.
#[derive(Clone)]
struct Headers {
    auth: Option<MetadataValue<Ascii>>,
    db: Option<MetadataValue<Ascii>>,
}

impl Interceptor for Headers {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(v) = &self.auth {
            req.metadata_mut().insert("authorization", v.clone());
        }
        if let Some(v) = &self.db {
            req.metadata_mut()
                .insert(chron_proto::DATABASE_HEADER, v.clone());
        }
        Ok(req)
    }
}

/// Events as the daemon sends them. A finite read's stream ends at the
/// read's end; a subscription's ends only with an error (the daemon
/// fenced or stopped).
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, ClientError>> + Send + 'static>>;

type Inner = ChronClient<InterceptedService<Channel, Headers>>;

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
    /// The leader's head when it took `epoch`; a follower adopts the
    /// epoch once its head reaches it.
    pub epoch_base: u64,
    /// The highest position a majority durably held when the chunk was
    /// cut: a follower's confirmed bound.
    pub confirmed: u64,
    /// Durable cursors that changed (every one on the opening chunk).
    pub cursors: Vec<CursorChange>,
    /// `cursors` is the complete list: a follower drops every cursor it
    /// holds that is not in it.
    pub all_cursors: bool,
    /// Stream definitions that changed (every one on the first chunk).
    pub streams: Vec<StreamChange>,
    /// `streams` is the complete list: a follower drops every stream it
    /// holds that is not in it.
    pub all_streams: bool,
}

/// A stream definition change on the replication stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamChange {
    /// The stream's name.
    pub name: String,
    /// The tags it covers (empty when deleted).
    pub tags: Vec<Bytes>,
    /// A removal.
    pub deleted: bool,
}

/// A durable cursor's state as the replication stream carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorChange {
    /// The cursor's name.
    pub name: String,
    /// The position after the last acknowledged event.
    pub next: u64,
    /// The session that owns it.
    pub session: u64,
    /// The cursor was deleted (`next` and `session` are then 0).
    pub deleted: bool,
}

/// The bytes of a backup archive fetched from a daemon, in order.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send + 'static>>;

/// A stream of replication chunks.
pub type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<ReplicationChunk, ClientError>> + Send + 'static>>;

/// The connection itself: swapped as one when the leader moves, so
/// every clone of the client (every database binding) follows.
struct Conn {
    addr: String,
    channel: Channel,
}

/// A connection to one daemon, bound to one of its databases. Cheap to
/// clone; every method takes `&self`. Made by [`Client::connect_any`],
/// it also knows the seeds to find the leader again.
#[derive(Clone)]
pub struct Client {
    conn: Arc<RwLock<Conn>>,
    config: ClientConfig,
    seeds: Option<Arc<Vec<String>>>,
    /// One search for the leader at a time; the generation says whether
    /// the connection was swapped while a caller waited for the lock.
    discovery: Arc<tokio::sync::Mutex<()>>,
    generation: Arc<AtomicU64>,
    token: Option<MetadataValue<Ascii>>,
    admin_token: Option<MetadataValue<Ascii>>,
    db: Option<MetadataValue<Ascii>>,
    database: Option<String>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Client({} at {})",
            self.database.as_deref().unwrap_or("default"),
            self.addr()
        )
    }
}

fn auth(token: Option<&str>) -> Result<Option<MetadataValue<Ascii>>, ClientError> {
    Ok(match token {
        Some(t) => Some(format!("Bearer {t}").parse().map_err(|_| {
            ClientError::InvalidArgument("the token is not valid header text".into())
        })?),
        None => None,
    })
}

fn db_header(database: Option<&str>) -> Result<Option<MetadataValue<Ascii>>, ClientError> {
    Ok(match database {
        Some(d) => Some(d.parse().map_err(|_| {
            ClientError::InvalidArgument(format!("{d:?} is not a valid database name"))
        })?),
        None => None,
    })
}

fn build(
    channel: &Channel,
    auth: &Option<MetadataValue<Ascii>>,
    db: &Option<MetadataValue<Ascii>>,
    n: usize,
) -> Inner {
    ChronClient::with_interceptor(
        channel.clone(),
        Headers {
            auth: auth.clone(),
            db: db.clone(),
        },
    )
    .max_decoding_message_size(n)
    .max_encoding_message_size(n)
}

/// Opens the channel to `addr` as `config` says.
async fn open(addr: &str, config: &ClientConfig) -> Result<Channel, ClientError> {
    let mut endpoint = Endpoint::from_shared(addr.to_string())
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
    Ok(endpoint.connect().await?)
}

/// An error with its causes, for a note: tonic's transport error says
/// only "transport error" on its own.
fn why(e: &ClientError) -> String {
    let mut text = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        text.push_str(": ");
        text.push_str(&s.to_string());
        source = s.source();
    }
    text
}

/// Opens `addr` and asks it who leads, both bounded by the connect
/// timeout; `Ok(None)` when it answered but names no leader.
async fn probe(
    addr: &str,
    config: &ClientConfig,
    token: &Option<MetadataValue<Ascii>>,
) -> Result<(Channel, pb::DiscoverResponse), ClientError> {
    let channel = open(addr, config).await?;
    let mut probe = build(&channel, token, &None, config.max_message_bytes);
    let d = tokio::time::timeout(
        config.connect_timeout,
        probe.discover(pb::DiscoverRequest {}),
    )
    .await
    .map_err(|_| {
        ClientError::Unavailable(format!(
            "no answer to Discover within {:?}",
            config.connect_timeout
        ))
    })??
    .into_inner();
    Ok((channel, d))
}

/// Asks each seed in turn who leads: a seed that leads itself (with a
/// live lease) is used directly, whatever address it advertises; a
/// seed naming another node is believed only once that node confirms
/// it leads. The members a seed names are asked after the seeds, so a
/// seed list need not be current. Each address is asked at most once
/// per search, at most 32 in all.
async fn find_leader(seeds: &[String], config: &ClientConfig) -> Result<Conn, ClientError> {
    let mut notes: Vec<String> = Vec::new();
    let mut asked: std::collections::HashSet<String> = std::collections::HashSet::new();
    let token = auth(config.token.as_deref())?;
    let mut queue: std::collections::VecDeque<String> = seeds.iter().cloned().collect();
    while let Some(seed) = queue.pop_front() {
        if asked.len() >= 32 {
            notes.push("32 addresses asked; giving up".into());
            break;
        }
        if !asked.insert(seed.clone()) {
            continue;
        }
        let (channel, d) = match probe(&seed, config, &token).await {
            Ok(x) => x,
            Err(e) => {
                notes.push(format!("{seed}: {}", why(&e)));
                continue;
            }
        };
        for p in &d.peers {
            if !asked.contains(p) && !queue.contains(p) {
                queue.push_back(p.clone());
            }
        }
        let role = Role::from_wire(d.role());
        let Some(leader) = d.leader_addr else {
            notes.push(format!(
                "{seed}: {role} at epoch {}, knows no leader",
                d.epoch
            ));
            continue;
        };
        if role == Role::Leader {
            return Ok(Conn {
                addr: seed.clone(),
                channel,
            });
        }
        if !asked.insert(leader.clone()) {
            notes.push(format!("{seed} names {leader}, already asked"));
            continue;
        }
        match probe(&leader, config, &token).await {
            Ok((channel, d)) if Role::from_wire(d.role()) == Role::Leader => {
                return Ok(Conn {
                    addr: leader,
                    channel,
                });
            }
            Ok((_, d)) => notes.push(format!(
                "{seed} names {leader}, which is {} at epoch {}",
                Role::from_wire(d.role()),
                d.epoch
            )),
            Err(e) => notes.push(format!(
                "{seed} names {leader}, which is unreachable: {}",
                why(&e)
            )),
        }
    }
    Err(ClientError::NoLeader(notes.join("; ")))
}

/// One finite read: what, from where, how many, which way.
enum Read {
    Aggregate(Uuid),
    All,
    Tag(Bytes),
    Stream(String),
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
        let channel = open(&addr, &config).await?;
        Self::assemble(Conn { addr, channel }, config, None)
    }

    /// Connects to the leader of a failover group: each seed (any node
    /// of the group) is asked who leads, in order, and the first that
    /// leads, or names a node that confirms it leads, wins.
    /// [`ClientError::NoLeader`] when none does. Commits that fail
    /// because the node no longer leads, or cannot be reached, look for
    /// the leader again and are retried there for up to
    /// `config.failover_timeout`, under the rule [`commit`](Client::commit)
    /// states; reads are not redirected. Every seed must use one scheme
    /// (`http://` or `https://`) and the TLS settings apply to all of
    /// them.
    pub async fn connect_any(
        seeds: impl IntoIterator<Item = impl Into<String>>,
        config: ClientConfig,
    ) -> Result<Client, ClientError> {
        let seeds: Vec<String> = seeds.into_iter().map(Into::into).collect();
        if seeds.is_empty() {
            return Err(ClientError::InvalidArgument("no addresses given".into()));
        }
        let conn = find_leader(&seeds, &config).await?;
        Self::assemble(conn, config, Some(Arc::new(seeds)))
    }

    fn assemble(
        conn: Conn,
        config: ClientConfig,
        seeds: Option<Arc<Vec<String>>>,
    ) -> Result<Client, ClientError> {
        let token = auth(config.token.as_deref())?;
        let admin_token = auth(config.admin_token.as_deref().or(config.token.as_deref()))?;
        let db = db_header(config.database.as_deref())?;
        Ok(Client {
            conn: Arc::new(RwLock::new(conn)),
            database: config.database.clone(),
            config,
            seeds,
            discovery: Arc::new(tokio::sync::Mutex::new(())),
            generation: Arc::new(AtomicU64::new(0)),
            token,
            admin_token,
            db,
        })
    }

    /// The address this client currently talks to (the leader found last,
    /// for one made by [`connect_any`](Client::connect_any)).
    pub fn addr(&self) -> String {
        self.conn
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .addr
            .clone()
    }

    /// The seeds a [`connect_any`](Client::connect_any) client was given.
    pub fn seeds(&self) -> &[String] {
        self.seeds.as_deref().map_or(&[], |s| s.as_slice())
    }

    /// Looks for the leader again among the seeds and switches every
    /// clone of this client to it. One search runs at a time: a caller
    /// that waited for another's search takes its result. Only for a
    /// client made by [`connect_any`](Client::connect_any); others fail
    /// with [`ClientError::InvalidArgument`].
    pub async fn rediscover(&self) -> Result<String, ClientError> {
        let Some(seeds) = &self.seeds else {
            return Err(ClientError::InvalidArgument(
                "this client was not made by connect_any".into(),
            ));
        };
        let before = self.generation.load(Ordering::Acquire);
        let _one = self.discovery.lock().await;
        if self.generation.load(Ordering::Acquire) != before {
            return Ok(self.addr());
        }
        let conn = find_leader(seeds, &self.config).await?;
        let addr = conn.addr.clone();
        *self.conn.write().unwrap_or_else(|p| p.into_inner()) = conn;
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(addr)
    }

    fn channel(&self) -> Channel {
        self.conn
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .channel
            .clone()
    }

    /// Whether a failed commit may be sent again to the leader found
    /// next without writing twice. With a commit id the daemon answers
    /// a resend with the first landing, so every failure that may have
    /// landed (a lost connection, a deadline, an unconfirmed sync
    /// commit) is retried, as is a refusal for the node's role; with an
    /// epoch token a refusal at an epoch above the token is final, since
    /// every leader from now on is above it too. Without an id: a
    /// refusal for the node's role came before any write; a connection
    /// failure may have come after it, so it is retried only when every
    /// append carries an expectation that makes a second landing a
    /// conflict; a commit with an epoch token is the caller's own
    /// fencing and is never retried.
    fn retryable(
        &self,
        e: &ClientError,
        epoch: Option<u64>,
        expectations: &[(Option<Uuid>, Expected)],
        has_id: bool,
    ) -> bool {
        if self.seeds.is_none() {
            return false;
        }
        if has_id {
            return match e {
                ClientError::Fenced { epoch: at, .. } => epoch.is_none_or(|token| *at <= token),
                ClientError::Unavailable(_)
                | ClientError::Transport(_)
                | ClientError::Unconfirmed { .. } => true,
                ClientError::Status(s) => s.code() == tonic::Code::DeadlineExceeded,
                _ => false,
            };
        }
        if epoch.is_some() {
            return false;
        }
        match e {
            ClientError::Fenced { .. } => true,
            ClientError::Unavailable(_) | ClientError::Transport(_) => {
                expectations.iter().all(|(agg, exp)| {
                    agg.is_some() && matches!(exp, Expected::Exact(_) | Expected::New)
                })
            }
            _ => false,
        }
    }

    /// The same connection bound to the database `name`: every call of
    /// the returned client carries `chron-db: name`. A name that is not
    /// valid header text makes every call fail with
    /// [`ClientError::InvalidArgument`]. The binding follows a leader
    /// swap of the client it came from.
    pub fn database(&self, name: impl Into<String>) -> Client {
        let name = name.into();
        let db = db_header(Some(&name)).unwrap_or_else(|_| {
            // Unrepresentable: send something the daemon will refuse.
            Some(MetadataValue::from_static("-invalid-"))
        });
        Client {
            conn: self.conn.clone(),
            config: self.config.clone(),
            seeds: self.seeds.clone(),
            discovery: self.discovery.clone(),
            generation: self.generation.clone(),
            token: self.token.clone(),
            admin_token: self.admin_token.clone(),
            db,
            database: Some(name),
        }
    }

    /// The database this client is bound to, `None` for the daemon's
    /// `default`.
    pub fn database_name(&self) -> Option<&str> {
        self.database.as_deref()
    }

    fn rpc(&self) -> Inner {
        build(
            &self.channel(),
            &self.token,
            &self.db,
            self.config.max_message_bytes,
        )
    }

    fn admin(&self) -> Inner {
        build(
            &self.channel(),
            &self.admin_token,
            &self.db,
            self.config.max_message_bytes,
        )
    }

    /// Commits every append atomically. [`ClientError::Conflict`] when an
    /// expectation fails; nothing is written then.
    ///
    /// On a client made by [`connect_any`](Client::connect_any) a commit
    /// the node refused for its role ([`ClientError::Fenced`]) was never
    /// written and is sent again to the leader found next.
    ///
    /// With a commit id (every commit has one unless
    /// [`ClientConfig::commit_ids`] is off or
    /// [`CommitOptions::commit_id`] names one) a commit that failed with
    /// [`ClientError::Unavailable`], [`ClientError::Transport`],
    /// [`ClientError::Unconfirmed`] or a deadline is sent again too: the
    /// leader found next answers it with the first landing
    /// ([`Committed::duplicate`]) if the write survived, and lands it
    /// once if it died with the old leader. An epoch token is sent
    /// along; a refusal at an epoch above it is final.
    ///
    /// Without an id, one that failed with [`ClientError::Unavailable`]
    /// or [`ClientError::Transport`] may have landed before the
    /// connection broke: it is sent again only when every append names
    /// an aggregate with [`Expected::Exact`] or [`Expected::New`], where
    /// a second landing is a [`ClientError::Conflict`] (one that may
    /// then report the caller's own earlier write); with
    /// [`Expected::Any`] or free events the error is returned and the
    /// caller decides. A timed-out or unconfirmed call is never retried,
    /// and neither is a commit with an epoch token.
    pub async fn commit(&self, txn: Transaction) -> Result<Committed, ClientError> {
        self.commit_with(txn, CommitOptions::default()).await
    }

    /// [`commit`](Client::commit) with an epoch token, a sync choice and
    /// a commit id of the caller's own.
    pub async fn commit_with(
        &self,
        txn: Transaction,
        options: CommitOptions,
    ) -> Result<Committed, ClientError> {
        let options = CommitOptions {
            sync: options.sync.or(self.config.sync_commit),
            commit_id: options
                .commit_id
                .or_else(|| self.config.commit_ids.then(|| Uuid::new_v4().into_bytes())),
            ..options
        };
        self.commit_inner(txn, options).await
    }

    /// Like [`commit`](Client::commit), carrying the epoch the writer
    /// expects the daemon to be at. [`ClientError::Fenced`] when it is not.
    pub async fn commit_with_epoch(
        &self,
        txn: Transaction,
        epoch: u64,
    ) -> Result<Committed, ClientError> {
        self.commit_with(
            txn,
            CommitOptions {
                epoch: Some(epoch),
                ..CommitOptions::default()
            },
        )
        .await
    }

    async fn commit_inner(
        &self,
        txn: Transaction,
        options: CommitOptions,
    ) -> Result<Committed, ClientError> {
        let epoch = options.epoch;
        let has_id = options.commit_id.is_some();
        let appends = txn.into_appends();
        let expectations: Vec<(Option<Uuid>, Expected)> =
            appends.iter().map(|a| (a.aggregate, a.expected)).collect();
        let wire: Vec<pb::Append> = appends
            .into_iter()
            .map(|a| convert::append_to_wire(a.aggregate, a.expected, a.events))
            .collect();
        let mut last = match self.commit_once(&wire, options, &expectations).await {
            Err(e) if self.retryable(&e, epoch, &expectations, has_id) => e,
            other => return other,
        };
        // Look for the leader until the deadline, resending only once one
        // is found; a search that fails keeps the commit's own error.
        let deadline = Instant::now() + self.config.failover_timeout;
        let mut backoff = Duration::from_millis(100);
        let mut search_failed: Option<ClientError> = None;
        while Instant::now() < deadline {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(1));
            match self.rediscover().await {
                Err(e) => search_failed = Some(e),
                Ok(_) => {
                    search_failed = None;
                    match self.commit_once(&wire, options, &expectations).await {
                        Err(e) if self.retryable(&e, epoch, &expectations, has_id) => last = e,
                        other => return other,
                    }
                }
            }
        }
        Err(match search_failed {
            Some(ClientError::NoLeader(notes)) => ClientError::NoLeader(format!(
                "{last}; no leader found within {:?}: {notes}",
                self.config.failover_timeout
            )),
            _ => last,
        })
    }

    async fn commit_once(
        &self,
        wire: &[pb::Append],
        options: CommitOptions,
        expectations: &[(Option<Uuid>, Expected)],
    ) -> Result<Committed, ClientError> {
        let resp = self
            .rpc()
            .commit(pb::CommitRequest {
                appends: wire.to_vec(),
                epoch: options.epoch,
                sync: options.sync,
                commit_id: options.commit_id.map(|i| i.to_vec()).unwrap_or_default(),
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

    /// The wire form of a read's options: the client's consistency when
    /// the call names none.
    fn read_wire(&self, o: ReadOptions) -> (i32, Option<u64>, Option<u64>) {
        let c = o.consistency.unwrap_or(self.config.consistency).wire() as i32;
        (c, o.at_least, o.wait_timeout.map(|d| d.as_millis() as u64))
    }

    async fn read(
        &self,
        read: Read,
        from: u64,
        limit: usize,
        backward: bool,
        opts: ReadOptions,
    ) -> Result<EventStream, ClientError> {
        let limit = limit as u64;
        let (consistency, at_least, wait_timeout_ms) = self.read_wire(opts);
        let stream = match read {
            Read::Aggregate(id) => self
                .rpc()
                .read_aggregate(pb::ReadAggregateRequest {
                    aggregate: id.as_bytes().to_vec(),
                    from_index: from,
                    limit,
                    backward,
                    consistency,
                    at_least,
                    wait_timeout_ms,
                })
                .await?
                .into_inner(),
            Read::All => self
                .rpc()
                .read_all(pb::ReadAllRequest {
                    from,
                    limit,
                    backward,
                    consistency,
                    at_least,
                    wait_timeout_ms,
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
                    consistency,
                    at_least,
                    wait_timeout_ms,
                })
                .await?
                .into_inner(),
            Read::Stream(name) => self
                .rpc()
                .read_stream(pb::ReadStreamRequest {
                    stream: name,
                    from,
                    limit,
                    backward,
                    consistency,
                    at_least,
                    wait_timeout_ms,
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
    /// [`ClientError::BelowFloor`] below the floor. The head here is the
    /// bound under the read's consistency.
    pub async fn read_all(&self, from: u64, limit: usize) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_all_stream(from, limit).await?).await
    }

    /// [`read_all`](Client::read_all) with [`ReadOptions`]: a
    /// consistency, or a position to wait for first.
    pub async fn read_all_with(
        &self,
        from: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read(Read::All, from, limit, false, opts).await?).await
    }

    /// [`read_all_backward`](Client::read_all_backward) with options.
    pub async fn read_all_backward_with(
        &self,
        to: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read(Read::All, to, limit, true, opts).await?).await
    }

    /// [`read_aggregate`](Client::read_aggregate) with options.
    pub async fn read_aggregate_with(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(
            self.read(Read::Aggregate(id), from_index, limit, false, opts)
                .await?,
        )
        .await
    }

    /// [`read_tag`](Client::read_tag) with options.
    pub async fn read_tag_with(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(
            self.read(Read::Tag(tag.into()), from, limit, false, opts)
                .await?,
        )
        .await
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
        self.read(
            Read::Aggregate(id),
            from_index,
            limit,
            false,
            ReadOptions::default(),
        )
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
        self.read(
            Read::Aggregate(id),
            to_index,
            limit,
            true,
            ReadOptions::default(),
        )
        .await
    }

    /// [`read_all`](Client::read_all) as a stream. A bad `from` fails the
    /// call, not the stream.
    pub async fn read_all_stream(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::All, from, limit, false, ReadOptions::default())
            .await
    }

    /// [`read_all_stream`](Client::read_all_stream) with options.
    pub async fn read_all_stream_with(
        &self,
        from: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::All, from, limit, false, opts).await
    }

    /// [`read_all_backward`](Client::read_all_backward) as a stream.
    pub async fn read_all_backward_stream(
        &self,
        to: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(Read::All, to, limit, true, ReadOptions::default())
            .await
    }

    /// [`read_tag`](Client::read_tag) as a stream.
    pub async fn read_tag_stream(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(
            Read::Tag(tag.into()),
            from,
            limit,
            false,
            ReadOptions::default(),
        )
        .await
    }

    /// [`read_tag_backward`](Client::read_tag_backward) as a stream.
    pub async fn read_tag_backward_stream(
        &self,
        tag: impl Into<Bytes>,
        to: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(
            Read::Tag(tag.into()),
            to,
            limit,
            true,
            ReadOptions::default(),
        )
        .await
    }

    /// The members of the stream `name` from position `from` on, at most
    /// `limit` (0 for all), in global order; below the floor, from the
    /// floor. An unknown stream is [`ClientError::NotFound`]; one still
    /// being filled [`ClientError::Unavailable`].
    pub async fn read_stream(
        &self,
        name: &str,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_stream_stream(name, from, limit).await?).await
    }

    /// [`read_stream`](Client::read_stream) under [`ReadOptions`].
    pub async fn read_stream_with(
        &self,
        name: &str,
        from: u64,
        limit: usize,
        opts: ReadOptions,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(
            self.read(Read::Stream(name.to_string()), from, limit, false, opts)
                .await?,
        )
        .await
    }

    /// Like [`read_stream`](Client::read_stream), newest first from
    /// position `to` (inclusive; `u64::MAX` for the end).
    pub async fn read_stream_backward(
        &self,
        name: &str,
        to: u64,
        limit: usize,
    ) -> Result<Vec<Event>, ClientError> {
        Self::collect(self.read_stream_backward_stream(name, to, limit).await?).await
    }

    /// [`read_stream`](Client::read_stream) as a stream.
    pub async fn read_stream_stream(
        &self,
        name: &str,
        from: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(
            Read::Stream(name.to_string()),
            from,
            limit,
            false,
            ReadOptions::default(),
        )
        .await
    }

    /// [`read_stream_backward`](Client::read_stream_backward) as a stream.
    pub async fn read_stream_backward_stream(
        &self,
        name: &str,
        to: u64,
        limit: usize,
    ) -> Result<EventStream, ClientError> {
        self.read(
            Read::Stream(name.to_string()),
            to,
            limit,
            true,
            ReadOptions::default(),
        )
        .await
    }

    /// Defines a stream (admin, on the leader): every event, committed
    /// before or after, carrying any of `tags` is a member. Returns once
    /// the earlier events are indexed. An existing name is
    /// [`ClientError::AlreadyExists`].
    pub async fn create_stream<T: Into<Bytes>>(
        &self,
        name: &str,
        tags: impl IntoIterator<Item = T>,
    ) -> Result<StreamInfo, ClientError> {
        let r = self
            .admin()
            .create_stream(pb::CreateStreamRequest {
                name: name.to_string(),
                tags: tags.into_iter().map(|t| t.into().to_vec()).collect(),
            })
            .await?
            .into_inner();
        r.stream
            .map(stream_info)
            .ok_or_else(|| ClientError::Decode("CreateStream answered no stream".into()))
    }

    /// Every stream of the database.
    pub async fn streams(&self) -> Result<Vec<StreamInfo>, ClientError> {
        Ok(self
            .rpc()
            .list_streams(pb::ListStreamsRequest {})
            .await?
            .into_inner()
            .streams
            .into_iter()
            .map(stream_info)
            .collect())
    }

    /// Removes a stream (admin, on the leader); its live subscriptions
    /// end. Whether it existed.
    pub async fn delete_stream(&self, name: &str) -> Result<bool, ClientError> {
        Ok(self
            .admin()
            .delete_stream(pb::DeleteStreamRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner()
            .existed)
    }

    /// How many events the aggregate has (0 for an unknown one).
    pub async fn aggregate_len(&self, id: Uuid) -> Result<u64, ClientError> {
        self.aggregate_len_with(id, ReadOptions::default()).await
    }

    /// [`aggregate_len`](Client::aggregate_len) with options: under the
    /// confirmed consistency, the events below the confirmed bound.
    pub async fn aggregate_len_with(
        &self,
        id: Uuid,
        opts: ReadOptions,
    ) -> Result<u64, ClientError> {
        let (consistency, at_least, wait_timeout_ms) = self.read_wire(opts);
        Ok(self
            .rpc()
            .aggregate_len(pb::AggregateLenRequest {
                aggregate: id.as_bytes().to_vec(),
                consistency,
                at_least,
                wait_timeout_ms,
            })
            .await?
            .into_inner()
            .len)
    }

    /// One past the last position a read would see: the bound under the
    /// client's consistency.
    pub async fn head(&self) -> Result<u64, ClientError> {
        Ok(self.head_with(ReadOptions::default()).await?.head)
    }

    /// The bounds, under the options' consistency and both others; with
    /// `at_least`, after waiting for that position.
    pub async fn head_with(&self, opts: ReadOptions) -> Result<Head, ClientError> {
        let (consistency, at_least, wait_timeout_ms) = self.read_wire(opts);
        let h = self
            .rpc()
            .head(pb::HeadRequest {
                consistency,
                at_least,
                wait_timeout_ms,
            })
            .await?
            .into_inner();
        Ok(Head {
            head: h.head,
            confirmed: h.confirmed,
            local: h.local,
        })
    }

    /// Committed events from position `from` on, then live ones as they
    /// commit (under the confirmed consistency, as a majority comes to
    /// hold them). The stream ends only with an error: [`ClientError::
    /// BelowFloor`] when `from` is below the floor, [`ClientError::
    /// Unavailable`] when the daemon is fenced or stops.
    pub async fn subscribe(&self, from: u64) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, Source::All, None).await
    }

    /// Like [`subscribe`](Client::subscribe), only the events carrying
    /// `tag`; below the floor it starts at the floor.
    pub async fn subscribe_tag(
        &self,
        tag: impl Into<Bytes>,
        from: u64,
    ) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, Source::Tag(tag.into()), None)
            .await
    }

    /// Like [`subscribe`](Client::subscribe), only the members of the
    /// stream `name`; below the floor it starts at the floor. The stream
    /// ends with [`ClientError::Unavailable`] if the stream is deleted.
    pub async fn subscribe_stream(
        &self,
        name: &str,
        from: u64,
    ) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, Source::Stream(name.to_string()), None)
            .await
    }

    /// [`subscribe`](Client::subscribe) (or, with `tag`,
    /// [`subscribe_tag`](Client::subscribe_tag)) under a consistency.
    pub async fn subscribe_with(
        &self,
        from: u64,
        tag: Option<Bytes>,
        consistency: Consistency,
    ) -> Result<EventStream, ClientError> {
        let source = tag.map_or(Source::All, Source::Tag);
        self.subscribe_inner(from, source, Some(consistency)).await
    }

    /// A subscription on any [`Source`] under a consistency.
    pub async fn subscribe_source(
        &self,
        source: Source,
        from: u64,
        consistency: Consistency,
    ) -> Result<EventStream, ClientError> {
        self.subscribe_inner(from, source, Some(consistency)).await
    }

    async fn subscribe_inner(
        &self,
        from: u64,
        source: Source,
        consistency: Option<Consistency>,
    ) -> Result<EventStream, ClientError> {
        let (tag, stream) = source_wire(source);
        let stream = self
            .rpc()
            .subscribe(pb::SubscribeRequest {
                from,
                tag,
                consistency: consistency.unwrap_or(self.config.consistency).wire() as i32,
                cursor: String::new(),
                stream,
            })
            .await?
            .into_inner();
        Ok(events(stream))
    }

    /// Subscribes on a durable cursor: the daemon keeps the position under
    /// `name`, so the stream resumes after a crash or a failover from the
    /// last acknowledged position (`from_if_new` for a new cursor; the
    /// floor when below it). Delivery is confirmed-only. The newest
    /// subscription on a cursor owns it: an older stream ends with
    /// [`ClientError::Unavailable`] "taken over", and its acknowledgments
    /// are refused as [`ClientError::TakenOver`]. Cursors are kept on the
    /// leader; a client made by [`connect_any`](Client::connect_any) tries
    /// the leader it finds next once when a node refuses. Acknowledge with
    /// [`Cursor::ack`]; the stream delivers again from the last
    /// acknowledgment, so processing is at least once.
    pub async fn subscribe_cursor(
        &self,
        name: &str,
        from_if_new: u64,
        tag: Option<Bytes>,
    ) -> Result<(EventStream, Cursor), ClientError> {
        self.subscribe_cursor_source(name, from_if_new, tag.map_or(Source::All, Source::Tag))
            .await
    }

    /// [`subscribe_cursor`](Client::subscribe_cursor) on any [`Source`].
    /// A cursor does not record its source: resuming it on another is
    /// the caller's call.
    pub async fn subscribe_cursor_source(
        &self,
        name: &str,
        from_if_new: u64,
        source: Source,
    ) -> Result<(EventStream, Cursor), ClientError> {
        let (tag, stream) = source_wire(source);
        let req = pb::SubscribeRequest {
            from: from_if_new,
            tag,
            consistency: pb::Consistency::Confirmed as i32,
            cursor: name.to_string(),
            stream,
        };
        let resp = match self.rpc().subscribe(req.clone()).await {
            Ok(r) => r,
            Err(s)
                if s.code() == tonic::Code::FailedPrecondition
                    && self.rediscover().await.is_ok() =>
            {
                self.rpc().subscribe(req).await?
            }
            Err(s) => return Err(s.into()),
        };
        let session = resp
            .metadata()
            .get(chron_proto::SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| ClientError::Decode("a cursor stream without a session".into()))?;
        let from = resp
            .metadata()
            .get(chron_proto::CURSOR_FROM_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(from_if_new);
        Ok((
            events(resp.into_inner()),
            Cursor {
                client: self.clone(),
                name: name.to_string(),
                session,
                from,
            },
        ))
    }

    /// Every durable cursor of the database and the confirmed bound.
    pub async fn cursors(&self) -> Result<CursorList, ClientError> {
        let l = self
            .rpc()
            .list_cursors(pb::ListCursorsRequest {})
            .await?
            .into_inner();
        Ok(CursorList {
            cursors: l
                .cursors
                .into_iter()
                .map(|c| CursorInfo {
                    name: c.name,
                    next: c.next,
                    session: c.session,
                })
                .collect(),
            confirmed: l.confirmed,
        })
    }

    /// Removes a durable cursor (admin); its live subscription ends.
    /// Whether it existed.
    pub async fn delete_cursor(&self, name: &str) -> Result<bool, ClientError> {
        Ok(self
            .admin()
            .delete_cursor(pb::DeleteCursorRequest {
                cursor: name.to_string(),
            })
            .await?
            .into_inner()
            .existed)
    }

    /// Syncs everything committed so far to disk.
    pub async fn flush(&self) -> Result<(), ClientError> {
        self.rpc().flush(pb::FlushRequest {}).await?;
        Ok(())
    }

    /// What the daemon reports about itself.
    pub async fn info(&self) -> Result<Info, ClientError> {
        let i = self.rpc().info(pb::InfoRequest {}).await?.into_inner();
        let role = Role::from_wire(i.role());
        let db_id = Uuid::from_slice(&i.db_id)
            .map_err(|_| ClientError::Decode("Info carries no database identity".into()))?;
        Ok(Info {
            database: i.database,
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
            leader_addr: i.leader_addr,
            lease_remaining: i.lease_remaining_ms.map(Duration::from_millis),
            peers: i.peers,
            voted_epoch: i.voted_epoch,
            majority_applied: i.majority_applied,
            followers: i
                .followers
                .into_iter()
                .map(|f| FollowerStatus {
                    peer: f.peer,
                    applied: f.applied,
                    durable: f.durable,
                    counted: f.counted,
                })
                .collect(),
            sync_commit: i.sync_commit,
            confirmed: i.confirmed,
            read_consistency: Consistency::from_wire(i.read_consistency),
            streams: i.streams,
            members_version: i.members_version,
            members_epoch: i.members_epoch,
            members_committed: i.members_committed,
            member: i.member,
        })
    }

    /// A full backup of the database as the archive's bytes, exactly what
    /// `chron restore` reads, from any node (no `--backup-dir` needed);
    /// the daemon's retention and other backups of the database wait
    /// while it streams. An admin call.
    pub async fn fetch_backup(&self) -> Result<ByteStream, ClientError> {
        let stream = self
            .admin()
            .fetch_backup(pb::FetchBackupRequest {})
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|item| Ok(Bytes::from(item?.data)))))
    }

    /// [`fetch_backup`](Client::fetch_backup) written to `path` through a
    /// `.tmp` file beside it, fsynced and renamed into place (an existing
    /// file at `path` is replaced); a failure leaves nothing new. Returns
    /// the archive's header.
    pub async fn fetch_backup_to(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<BackupMeta, ClientError> {
        use tokio::io::AsyncWriteExt;
        let path = path.as_ref();
        let tmp = std::path::PathBuf::from(format!("{}.tmp", path.display()));
        let mut stream = self.fetch_backup().await?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        let written: Result<(), ClientError> = async {
            while let Some(chunk) = stream.next().await {
                file.write_all(&chunk?).await?;
            }
            file.flush().await?;
            file.sync_all().await?;
            Ok(())
        }
        .await;
        drop(file);
        if let Err(e) = written {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        tokio::fs::rename(&tmp, path).await?;
        chron::inspect_backup(path).map_err(|e| ClientError::Decode(e.to_string()))
    }

    /// Replication with acknowledgments (daemon to daemon): the stream of
    /// chunks and the side to report applied heads on. `peer_addr` is the
    /// follower's advertised address, `durable` whether it fsyncs what it
    /// applies. An admin call; a daemon before 0.4.0 answers
    /// `Unimplemented`.
    #[doc(hidden)]
    pub async fn follow(
        &self,
        from: u64,
        last_fingerprint: Option<u32>,
        peer_addr: String,
        durable: bool,
    ) -> Result<(ChunkStream, FollowAcks), ClientError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<pb::FollowRequest>(16);
        tx.send(pb::FollowRequest {
            msg: Some(pb::follow_request::Msg::Open(pb::FollowOpen {
                from_position: from,
                last_fingerprint,
                peer_addr,
                durable,
            })),
        })
        .await
        .map_err(|_| ClientError::Unavailable("the follow channel closed".into()))?;
        let stream = self
            .admin()
            .follow(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?
            .into_inner();
        Ok((chunks(stream), FollowAcks(tx)))
    }

    /// Who leads, as the daemon knows it. Open to any token.
    pub async fn discover(&self) -> Result<Discovered, ClientError> {
        let d = self
            .rpc()
            .discover(pb::DiscoverRequest {})
            .await?
            .into_inner();
        let role = Role::from_wire(d.role());
        Ok(Discovered {
            leader_addr: d.leader_addr,
            epoch: d.epoch,
            role,
            peers: d.peers,
            lease_remaining: d.lease_remaining_ms.map(Duration::from_millis),
            members_version: d.members_version,
            members_epoch: d.members_epoch,
        })
    }

    /// The group's membership as the daemon holds it. Open to any token.
    pub async fn members(&self) -> Result<MembersStatus, ClientError> {
        let m = self
            .rpc()
            .get_members(pb::GetMembersRequest {})
            .await?
            .into_inner();
        Ok(MembersStatus {
            members: Members::from_wire(m.members),
            committed: m.committed,
            member: m.member,
            leader_addr: m.leader_addr,
        })
    }

    /// Adds `addr` to the group's membership, on the leader; `force`
    /// adds a node that is not following the leader yet. Returns the new
    /// membership and whether it is committed already (a majority of it
    /// holds it); when not, [`members`](Client::members) says when it
    /// is. An admin call.
    pub async fn add_member(
        &self,
        addr: &str,
        force: bool,
    ) -> Result<(Members, bool), ClientError> {
        self.change_members(
            pb::change_members_request::Change::Add(addr.to_string()),
            force,
        )
        .await
    }

    /// Removes `addr` from the group's membership, on the leader (the
    /// leader itself included: it steps down once the change commits).
    /// An admin call.
    pub async fn remove_member(&self, addr: &str) -> Result<(Members, bool), ClientError> {
        self.change_members(
            pb::change_members_request::Change::Remove(addr.to_string()),
            false,
        )
        .await
    }

    async fn change_members(
        &self,
        change: pb::change_members_request::Change,
        force: bool,
    ) -> Result<(Members, bool), ClientError> {
        let r = self
            .admin()
            .change_members(pb::ChangeMembersRequest {
                change: Some(change),
                force,
            })
            .await?
            .into_inner();
        Ok((Members::from_wire(r.members), r.committed))
    }

    /// A leader's heartbeat to a peer (daemon to daemon). An admin call.
    #[doc(hidden)]
    pub async fn heartbeat(
        &self,
        req: pb::HeartbeatRequest,
    ) -> Result<pb::HeartbeatResponse, ClientError> {
        Ok(self.admin().heartbeat(req).await?.into_inner())
    }

    /// The daemon's stored crc32 of the records at `positions` (`None`
    /// past the head or below the floor): how a copy finds where its
    /// history parts from the daemon's. An admin call.
    pub async fn fingerprints(&self, positions: &[u64]) -> Result<Vec<Option<u32>>, ClientError> {
        let r = self
            .admin()
            .fingerprints(pb::FingerprintsRequest {
                positions: positions.to_vec(),
            })
            .await?
            .into_inner();
        Ok(r.fingerprints.into_iter().map(|f| f.crc).collect())
    }

    /// A candidate's request for a vote (daemon to daemon). An admin call.
    #[doc(hidden)]
    pub async fn request_vote(
        &self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, ClientError> {
        Ok(self.admin().request_vote(req).await?.into_inner())
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
        Ok(chunks(stream))
    }

    /// Creates the empty database `name` on the daemon (the leader). An
    /// admin call; [`ClientError::AlreadyExists`] when it exists.
    pub async fn create_database(&self, name: &str) -> Result<DatabaseInfo, ClientError> {
        self.create_database_with(name, DatabaseOptions::default())
            .await
    }

    /// [`create_database`](Client::create_database) with the database's
    /// own fsync setting and record limit.
    pub async fn create_database_with(
        &self,
        name: &str,
        options: DatabaseOptions,
    ) -> Result<DatabaseInfo, ClientError> {
        let r = self
            .admin()
            .create_database(pb::CreateDatabaseRequest {
                name: name.to_string(),
                options: Some(pb::DatabaseOptions {
                    fsync: options.fsync,
                    max_record_bytes: options.max_record_bytes,
                }),
            })
            .await?
            .into_inner();
        database_info(
            r.database
                .ok_or_else(|| ClientError::Decode("CreateDatabase without a database".into()))?,
        )
    }

    /// Every database of the daemon. An admin call.
    pub async fn list_databases(&self) -> Result<Vec<DatabaseInfo>, ClientError> {
        Ok(self.list_databases_full().await?.databases)
    }

    /// Every database and the daemon's recent drops (admin).
    pub async fn list_databases_full(&self) -> Result<Listing, ClientError> {
        let r = self
            .admin()
            .list_databases(pb::ListDatabasesRequest {})
            .await?
            .into_inner();
        Ok(Listing {
            databases: r
                .databases
                .into_iter()
                .map(database_info)
                .collect::<Result<_, _>>()?,
            dropped: r
                .dropped
                .iter()
                .filter_map(|t| {
                    Some(Tombstone {
                        db_id: Uuid::from_slice(&t.db_id).ok()?,
                        name: t.name.clone(),
                        dropped_at: t.dropped_at_unix_secs,
                    })
                })
                .collect(),
        })
    }

    /// Drops the database `name` on the daemon (the leader); `default`
    /// cannot be dropped. An admin call; [`ClientError::NotFound`] when
    /// there is no such database.
    pub async fn drop_database(&self, name: &str) -> Result<(), ClientError> {
        self.admin()
            .drop_database(pb::DropDatabaseRequest {
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    /// Whether `service` reports SERVING: `""` for the daemon as a whole,
    /// `"chron.v1.Chron"` for the service, `"chron.v1.Chron.leader"` only
    /// while the node leads. Needs no token.
    pub async fn health(&self, service: &str) -> Result<bool, ClientError> {
        let r = HealthClient::new(self.channel())
            .check(HealthCheckRequest {
                service: service.to_string(),
            })
            .await?
            .into_inner();
        Ok(r.status() == ServingStatus::Serving)
    }
}

/// A source as the wire carries it: the tag, and the stream name (empty
/// for none).
fn source_wire(source: Source) -> (Option<Vec<u8>>, String) {
    match source {
        Source::All => (None, String::new()),
        Source::Tag(t) => (Some(t.to_vec()), String::new()),
        Source::Stream(name) => (None, name),
    }
}

fn stream_info(s: pb::StreamInfo) -> StreamInfo {
    StreamInfo {
        name: s.name,
        tags: s.tags.into_iter().map(Bytes::from).collect(),
        ready: s.ready,
    }
}

/// Wire chunks as library chunks.
fn chunks(stream: tonic::Streaming<pb::ReplicationChunk>) -> ChunkStream {
    Box::pin(stream.map(|item| {
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
            epoch_base: c.epoch_base,
            confirmed: c.confirmed,
            cursors: c
                .cursors
                .into_iter()
                .map(|s| CursorChange {
                    name: s.name,
                    next: s.next,
                    session: s.session,
                    deleted: s.deleted,
                })
                .collect(),
            all_cursors: c.all_cursors,
            streams: c
                .streams
                .into_iter()
                .map(|s| StreamChange {
                    name: s.name,
                    tags: s.tags.into_iter().map(Bytes::from).collect(),
                    deleted: s.deleted,
                })
                .collect(),
            all_streams: c.all_streams,
        })
    }))
}

fn database_info(d: pb::DatabaseInfo) -> Result<DatabaseInfo, ClientError> {
    Ok(DatabaseInfo {
        name: d.name,
        head: d.head,
        floor: d.floor,
        epoch: d.epoch,
        db_id: Uuid::from_slice(&d.db_id)
            .map_err(|_| ClientError::Decode("a database without an identity".into()))?,
        fsync: d.options.as_ref().map(|o| o.fsync),
        max_record_bytes: d.options.as_ref().map(|o| o.max_record_bytes),
    })
}
