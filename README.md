# chron

An event-sourcing database: a Rust library, a gRPC daemon, a typed client
and a CLI.

| Crate (crates.io) | What |
|---|---|
| `chron-db` | the embeddable library; its Rust name is `chron` (`use chron::Db`) |
| `chron-proto` | the `chron.v1` gRPC contract and generated code |
| `chrond` | the daemon: one database behind that service, as a leader or a replica |
| `chron-client` | a typed async client in the library's vocabulary |
| `chron-cli` | the `chron` binary: client and offline maintenance |
| `examples/bank` | accounts as aggregates, embedded or through a daemon (not published) |

## Install

```
cargo add chron-db                 # the library; code says `use chron::...`
cargo add chron-client             # the client; no protoc needed
cargo install chrond chron-cli     # the daemon and the `chron` binary
```

The crates.io name `chron` belongs to another crate, so the library is
published as `chron-db` with its library target named `chron`. A project
that also depends on that other `chron` renames one of them, for example
`chron = { package = "chron-db", version = "0.1" }`. Prebuilt `chrond` and
`chron` binaries for Linux (x86_64 and aarch64, static musl) and macOS
(Apple silicon) are attached to each
[GitHub Release](https://github.com/wave-cl/chron/releases) with a
`SHA256SUMS` file.

## The library

Events live in append-only logs called **aggregates**, each keyed by a
binary UUID. A **transaction** appends to any number of aggregates at once
and commits atomically; each append states what length it expects the
aggregate to have (`Exact(n)`, `New`, or `Any`), and if any expectation
fails the whole transaction is refused and nothing is written. Every event
also takes a dense position in **one global order**, readable sequentially
from any position, and carries any number of **tags**; reading a tag yields
every event carrying it, in global order. An event may also belong to no
aggregate at all (a **free** event); it still takes a global position and
tags and commits with the rest of its transaction.

```rust
use chron::{Db, Expected, NewEvent, Options, Uuid};

let db = Db::open("./orders.chron", Options::new())?;
let order = Uuid::new_v4();     // needs `uuid` with its `v4` feature; chron takes any Uuid
let customer = Uuid::new_v4();

let mut txn = db.transaction();
txn.append_one(order, Expected::New, NewEvent::new(r#"{"placed":true}"#).tag("orders"));
txn.append_one(customer, Expected::Exact(3), NewEvent::new(r#"{"ordered":1}"#).tag("customers"));
txn.append_free(vec![NewEvent::new("audit: order placed").tag("audit")]);
let committed = txn.commit()?;          // Err(Error::ExpectedLength{..}) if `customer` is not at 3

let events = db.read_aggregate(order, 0, 100)?;      // from index 0, at most 100
let everything = db.read_all(committed.first, 0)?;   // from a global position; 0 = all
let audit = db.read_tag("audit", 0, 100)?;            // the tagged subset, in global order
let newest = db.read_all_backward(u64::MAX, 10)?;     // the last ten, newest first

for event in db.iter_tag("orders", 0) {               // a page at a time, to the head
    let event = event?;
    println!("{} {:?}", event.position, event.payload);
}
for event in db.subscribe_tag("orders", 0) {          // the same, then live, never ends
    let event = event?;
    println!("{} {:?}", event.position, event.payload);
}
```

Every read takes `(from, limit)` with `limit` 0 meaning everything, and
has a backward twin (`read_all_backward`, `read_aggregate_backward`,
`read_tag_backward`) that takes an inclusive upper bound and returns
newest first; `u64::MAX` means "from the end". `iter_all`,
`iter_aggregate` and `iter_tag` page to the head without holding it all.
Tags are stored sorted and deduplicated.

## Guarantees

- **Atomic transactions.** All of a transaction's events become visible
  together, at contiguous global positions, or none do. The first and last
  event of a transaction carry flags so a sequential reader sees the
  boundaries.
- **Optimistic concurrency per aggregate.** `Expected::Exact(n)` fails with
  `Error::ExpectedLength { aggregate, expected, actual }` unless the
  aggregate has exactly `n` events; the same aggregate may appear twice in a
  transaction and the second expectation is checked after the first append.
- **Durability.** Under `Fsync::Always` (the default) a committed
  transaction survives power loss. Under `Fsync::Never` it survives a
  process crash; `flush()` (also run on drop) makes it durable, and every
  maintenance write (retention, epoch, identity) syncs what it depends on.
- **Recovery.** Event bytes go to append-only segment files and the indexes
  to one redb file; the redb commit is the commit point. On open, anything
  in the segments past the committed head was never acknowledged and is
  truncated; a failed commit truncates its own bytes at once. Damage inside
  committed data is `Error::Corrupt`; index and data disagreeing is
  `Error::Inconsistent`; neither is repaired silently.
- **One writer, many readers.** `Db` is cheap to clone and shared between
  threads; commits serialise on one lock, reads never take it. A `LOCK`
  file refuses a second process. Transactions committed concurrently are
  landed together (one segment write, one fsync, one index commit for up
  to `Options::group_commit_max_txns` of them), each still checked and
  acknowledged on its own; `Db::stats()` counts batches, transactions,
  events and fsyncs.
- **Live subscriptions.** `subscribe(from)` and `subscribe_tag(tag, from)`
  return a `Subscription`: an iterator that catches up through the index
  and then blocks for new commits. `next_timeout` serves threads that also
  watch a stop flag; `position()` is the checkpoint to resume from. A
  subscription keeps the database open. Starting below the retention
  floor, `subscribe` yields one `Error::BelowFloor` and ends;
  `subscribe_tag` starts at the floor.

## Limits

Tags are at most 255 bytes and 255 per event. One event's record is at most
`Options::max_record_bytes` (16 MiB by default). Segments roll at
`Options::segment_max_bytes` (256 MiB by default); a transaction is never
split across segments. At most `Options::max_open_segments` (64) segment
files stay open for reading; the least recently used is closed first.

## Layout on disk

```
<path>/
  LOCK                      held while a Db is open
  index.redb                head, floor, identity, epoch, aggregate lengths,
                            (aggregate, index) → position,
                            position → (segment, offset, length), (tag, position)
  segments/<base>.seg       64-byte header, then framed records (length, crc32, body)
```

A daemon's data directory holds one such directory per database
(`<data_dir>/<name>/`, `default` always) with its markers (`promoted`,
`fenced`, `replica`) at the root. A 0.1.0 data directory, which was one
database at the root, is moved into `default/` on the daemon's first
start.

Every database has an **identity** (a UUID, `db.id()`), kept by backups,
restores and index rebuilds, and an **epoch** (`db.epoch()`) that
replication raises on every promotion. `db.info()` reports head, floor,
segment count and bytes, identity, epoch and the fsync mode.

## Backups, retention, checking

- `db.backup(archive)` writes a **full** backup while the database stays in
  use: each segment is copied up to where the index places its last
  committed record, plus every aggregate's length (so lengths of aggregates
  wholly below the floor survive a restore).
- `db.backup_since(archive, since)` writes an **incremental** backup: the
  frames of positions `since..head`, to be applied onto a database whose
  head is `since`.
- `chron::restore_backup(archive, dir, &opts)` restores a full backup into
  a directory that must not exist: the segments go to a staging directory,
  the archive's CRC is checked, the index is **rebuilt** from the
  segments, and only then is the staging directory renamed into place.
- `db.apply_backup(archive)` appends an incremental backup's events after
  checking every frame; the head must equal the archive's base head
  (`Error::WrongBaseHead`), and a damaged archive changes nothing. Giving
  either call the other kind is `Error::WrongBackupKind`.
- `chron::inspect_backup(archive)` reads the header only.

The archive is one file: a magic, a JSON header (`BackupMeta`: kind,
base head, head, floor, identity, time, counts), the entries, and a CRC32
trailer over everything before it.

`db.retain_from(floor)` drops every segment that lies wholly below
`floor`. The floor in force rounds **down** to a segment boundary (the
base of the first kept segment), positions never renumber, and the tail
segment is never dropped. Reads below the floor fail with
`Error::BelowFloor`; backward reads stop at it; aggregate and tag reads
return what survives; aggregate lengths are unchanged, so
`Expected::Exact` keeps working. Readers and subscriptions take the floor
inside their own index snapshot, so a retention racing them is either
wholly before or wholly after each read.

- `Options::verify_all_segments(true)` reads every segment on open, checking
  each record's CRC, that positions run consecutively, and that the index
  places every record where it is. Off by default, when only the tail is
  checked (recovery may truncate unacknowledged bytes there).
- `Db::rebuild_index(path, &opts)` rebuilds `index.redb` from the segment
  files alone, for a missing or damaged index. It takes `LOCK` (no `Db`
  may be open), writes the new index beside the old one and renames it
  into place only when complete. Every complete transaction in the
  segments is indexed, including a trailing one whose commit was never
  acknowledged; a torn trailing transaction is dropped and reported
  (`Rebuilt::tail_bytes_dropped`, `tail_reason`). Damage anywhere else,
  including inside the last segment when committed records follow it, is
  reported, never repaired. A first segment above zero is read as a
  retention floor; the old index's identity and aggregate lengths are
  kept when it can still be read.

## Async

With the `tokio` feature, head changes are also published on a tokio
watch channel: `Db::wait_for_async(min_head)`, `Subscription::next_async()`
(cancel-safe at its one await, so `tokio::time::timeout(d,
sub.next_async())` is the timeout idiom) and `Subscription::wait_async()`
with `catch_up()` / `pop()` for tasks that want the reads off their own
thread. The daemon uses it; the sync API is unchanged and the feature is
off by default.

## The client crate

```rust
use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};

let client = Client::connect("http://127.0.0.1:4242", ClientConfig::default()).await?;
let order = Uuid::new_v4();
let mut txn = Transaction::new();
txn.append_one(order, Expected::New, NewEvent::new("placed").tag("orders"));
let committed = client.commit(txn).await?;        // Err(ClientError::Conflict { actual, expected, .. })
let events = client.read_aggregate(order, 0, 0).await?;
let mut page = client.read_all_stream(0, 0).await?;  // an EventStream, for reads too large to hold
let mut live = client.subscribe_tag("orders", committed.next).await?;
```

`chron-client` speaks the library's types and shapes: the same six reads
with `limit` 0 for everything, each with a `_stream` variant,
`subscribe` / `subscribe_tag`, `info`, `flush`, the admin calls
(`backup`, `apply_backup`, `retain`, `promote`, `fence`, `replicate`)
and `create_database` / `list_databases` / `drop_database`. A client is
bound to one database: `ClientConfig.database`, or
`client.database("orders")` for another binding over the same
connection.
Errors are typed: `Conflict`, `Fenced { epoch }`, `PositionOutOfRange
{ position, head }`, `BelowFloor { position, floor }`, `Unauthenticated`,
`PermissionDenied`, `ResourceExhausted`, `Unavailable`. `ClientConfig`
takes the TLS settings (`ca_pem`, an `Identity`, `domain`; only for
`https://`), `token`, `admin_token`, `connect_timeout`, `request_timeout`
and `max_message_bytes`. Code written against an embedded `chron::Db`
moves to a daemon by swapping the handle; `examples/bank` does exactly
that behind a small `Store` trait.

## The daemon

```
cargo run -p chrond -- --data-dir ./orders.chron            # 127.0.0.1:4242
chrond --data-dir DIR --listen 0.0.0.0:4242 [--no-fsync] [--verify-all-segments]
       [--tls-cert server.pem --tls-key server.key [--tls-client-ca ca.pem]]
       [--token SECRET | --token-file FILE] [--admin-token SECRET | --admin-token-file FILE]
       [--backup-dir DIR]
       [--max-message-bytes N] [--request-timeout SECS] [--concurrency-per-connection N]
       [--keepalive-interval SECS] [--keepalive-timeout SECS]
       [--metrics-listen 127.0.0.1:9100] [--http-listen 127.0.0.1:8080]
       [--log-format text|json] [--discover-interval SECS]
       [--replicate-from http://leader:4242 [--replicate-token SECRET | --replicate-token-file FILE]
        [--replicate-ca ca.pem] [--replicate-cert c.pem --replicate-key c.key] [--replicate-domain NAME]]
       [--promote]
```

Flags fall back to environment variables: `CHRON_DATA_DIR`,
`CHRON_LISTEN`, `CHROND_TLS_CERT` / `_KEY` / `_CLIENT_CA`, `CHRON_TOKEN`,
`CHRON_TOKEN_FILE`, `CHROND_ADMIN_TOKEN`, `CHROND_ADMIN_TOKEN_FILE`,
`CHROND_BACKUP_DIR`, `CHROND_MAX_MESSAGE_BYTES`, `CHROND_REQUEST_TIMEOUT`,
`CHROND_KEEPALIVE_INTERVAL`, `CHROND_KEEPALIVE_TIMEOUT`,
`CHROND_METRICS_LISTEN`, `CHROND_HTTP_LISTEN`, `CHROND_LOG_FORMAT`,
`CHROND_DISCOVER_INTERVAL`,
`CHRON_REPLICATE_FROM`, `CHRON_REPLICATE_TOKEN`, `CHRON_REPLICATE_TOKEN_FILE`,
`CHROND_REPLICATE_CA` / `_CERT` / `_KEY` / `_DOMAIN`. Logging follows
`CHROND_LOG` (an `EnvFilter`, default `info`) on stderr, one line per call
with its method, peer, gRPC status and duration; `--log-format json`
writes one JSON object per line (`timestamp`, `level`, `target`,
`message`, the event's fields, and `span.method` / `span.peer` inside a
call). Ctrl-C or SIGTERM stops
it: every stream ends with `UNAVAILABLE`, the server drains, the database
is flushed.

The service is `chron.v1.Chron` in
`crates/chron-proto/proto/chron/v1/chron.proto`: commit, the six reads
(server-streamed, `limit` 0 for everything), subscribe, head, aggregate
length, info, flush, backup, apply, retain, replicate, promote, fence,
and CreateDatabase, ListDatabases and DropDatabase. One daemon serves
**several databases**: a call names its database with the metadata
`chron-db: <name>` (`[a-z0-9_-]{1,64}`); absent, it goes to `default`,
which always exists and cannot be dropped, so a client that knows
nothing of databases keeps working. An unknown name is `NOT_FOUND`, a
bad one `INVALID_ARGUMENT`, creating an existing one `ALREADY_EXISTS`.
Databases are created and dropped on the leader (admin calls); dropping
one ends its streams with `UNAVAILABLE` and removes its directory.
Options (fsync, record limit) are daemon-wide; backups go to
`<backup_dir>/<name>/<file>`.
A failed expectation is `FAILED_PRECONDITION` with the metadata
`chron-actual-length` and `chron-aggregate`; a write refused by the node's
role or epoch is `FAILED_PRECONDITION` with `chron-epoch`; bad requests
are `INVALID_ARGUMENT`; positions past the head or below the floor are
`OUT_OF_RANGE` with `chron-position` and `chron-head` or `chron-floor`;
a missing token is `UNAUTHENTICATED`, a missing admin token
`PERMISSION_DENIED`, a transaction above the message limit
`RESOURCE_EXHAUSTED`.

Plaintext and open by default; off a loopback address without a token or
TLS the daemon logs a warning. With `--tls-client-ca` only clients
presenting a certificate from that CA are accepted. With `--token`, every
`chron.v1.Chron` call must carry `authorization: Bearer <token>`
(compared in constant time, scheme case-insensitive). With
`--admin-token` as well, promote, fence, retain, backup, apply and
replicate need that token instead (it opens every other call too).
Backups over the daemon are taken and applied by bare file name under
`--backup-dir`; without one those calls are refused. `grpc.health.v1.Health`
stays open: `""` and `chron.v1.Chron` report SERVING until shutdown, and
`chron.v1.Chron.leader` only while the node leads, so
`grpc_health_probe -addr host:4242 -service chron.v1.Chron.leader` finds
the writable node. Messages above `--max-message-bytes` (64 MiB) are
refused; the limit must cover the database's `max_record_bytes`. HTTP/2
keepalives run every 30 s; `--request-timeout` bounds a call's response
head (streams keep running) and is off by default. Finite reads are paged
off the blocking pool and streamed under back-pressure, so stalled
clients hold no thread.

### The HTTP gateway

`--http-listen ADDR` serves the same database over HTTP/1.1 and JSON for
clients without gRPC (curl, browsers, scripts). The shapes are the CLI's
`--json` shapes, so what `chron read --json` prints is what the gateway
returns. The document at `/openapi.json` (OpenAPI 3.1) lists every route;
a unit test keeps it equal to the router.

```
POST   /v1/db/{db}/commit?epoch=N            {"appends":[{"aggregate":UUID|"free","expect":"any"|"new"|N,
                                              "events":[{"payload":..,"metadata":..,"tags":[..]}]}]}
GET    /v1/db/{db}/events?from&limit&backward
GET    /v1/db/{db}/aggregates/{uuid}/events?from&limit&backward
GET    /v1/db/{db}/aggregates/{uuid}/len
GET    /v1/db/{db}/tags/{tag}/events?from&limit&backward
GET    /v1/db/{db}/head                      {"head":N}
GET    /v1/db/{db}/info
GET    /v1/db/{db}/tail?from&tag             text/event-stream
POST   /v1/db/{db}/flush                     204
POST   /v1/db/{db}/retain                    {"floor":N}            (admin)
GET    /v1/databases
POST   /v1/databases                         {"name":".."}  201     (admin)
DELETE /v1/databases/{name}                  204                    (admin)
GET    /v1/status
GET    /openapi.json, GET /                  (no token)
```

```sh
curl -s -XPOST localhost:8080/v1/db/default/commit -H 'authorization: Bearer S' \
  -d '{"appends":[{"aggregate":"free","events":[{"payload":"hello"}]}]}'
curl -s 'localhost:8080/v1/db/default/events?from=0&limit=10' -H 'authorization: Bearer S'
curl -sN 'localhost:8080/v1/db/default/tail?from=0' -H 'authorization: Bearer S'
```

Reads return `{"events":[..],"next":POSITION|null}`; `limit` is at most
4096 and defaults to 256; `next` is `null` once the log is exhausted.
Binary payloads, metadata and tags come back as `payload_base64` and
friends, and may be sent that way. `tail` is server-sent events: one
`data:` line of JSON per event, a `: heartbeat` comment every 15 s, and
a final `event: error` when the database is dropped, the node fenced or
the daemon stops. Errors are `{"error","code",...}` with the gRPC code's
name (`FailedPrecondition`, `OutOfRange`, …) and the same metadata as
the gRPC call (`actual_length`, `aggregate`, `epoch`, `position`,
`head`, `floor`), as HTTP 400, 401, 403, 404, 409 (a failed expectation
or an existing database), 413, 416, 503 or 500.

The gateway shares the daemon's tokens and TLS: with `--token` every
`/v1` route needs `authorization: Bearer <token>`, the admin routes the
admin token, and with `--tls-cert` the gateway is HTTPS (a client CA is
enforced there too). Bodies are bounded by `--max-message-bytes`,
a response's head by `--request-timeout` (408; a `tail` stream keeps
running once it has started), and
`chron_http_requests_total{route,status}` counts the calls. Off by
default; on a non-loopback address without a token or TLS the daemon
warns.

## The CLI

```
chron append <AGGREGATE|free> [--expect any|new|N] [--tag T]... [--metadata M] (-d PAYLOAD... | --stdin)   # every -d (or stdin line) is one event, all in one transaction
chron commit (-f FILE | --stdin)   # {"appends":[{"aggregate":"<uuid>"|null,"expect":"new","events":[{"payload":"…","tags":["…"]}]}]}
chron read <AGGREGATE> [--from N] [--limit N] [--backward]
chron all [--from N] [--limit N] [--backward]
chron tag <TAG> [--from N] [--limit N] [--backward]
chron tail [--from N | --from-head] [--tag T]        # from the floor by default
chron len <AGGREGATE> | head | info | flush
chron status [--max-lag N]                           # exit 0 healthy, 1 degraded, 2 unreachable
chron backup <NAME> [--since N] | apply <NAME>       # names under the daemon's --backup-dir
chron retain <FLOOR>
chron health [--service NAME]                        # exit 0 SERVING, 1 NOT_SERVING, 2 unreachable
chron promote | fence <EPOCH>                        # replication
chron db create <NAME> | db list | db drop <NAME>    # databases; --db NAME (CHRON_DB) addresses one
chron append --epoch N … | commit --epoch N …        # fencing token
chron rebuild-index <DIR> | verify <DIR> | restore <ARCHIVE> <DIR> | inspect <ARCHIVE>   # offline, no daemon
```

`--addr` (or `CHRON_ADDR`, default `http://127.0.0.1:4242`) names the
daemon; `https://` enables TLS with `--tls-ca`, `--tls-cert`/`--tls-key`
and `--tls-domain` (`CHRON_TLS_*`); `--token` (`CHRON_TOKEN`) and
`--admin-token` (`CHRON_ADMIN_TOKEN`) send the bearer tokens;
`--max-message-bytes` (`CHRON_MAX_MESSAGE_BYTES`) matches the daemon's
limit; `--db` (`CHRON_DB`) addresses a database other than `default`. The offline commands take `--max-record-bytes` when the database
was written with a non-default limit, and refuse a directory that holds
no database. `--json` prints one JSON object per line, with `payload`,
`metadata` and `tags` as strings when they are UTF-8 and as
`payload_base64` / `metadata_base64` / `tags_base64` otherwise (the same
keys are accepted by `commit`); an empty payload or metadata is `null`;
an error is `{"error","code"}` on stderr. Exit codes: 0 ok, 1 the daemon
refused or an offline command failed, 2 usage (including unreadable
files and bad base64) or an unreachable daemon.

## Operations

`--metrics-listen ADDR` serves Prometheus metrics at `GET /metrics` over
plain HTTP with no token: keep it on a private interface. Counters are
updated as things happen (`chron_rpc_calls_total{db,method,code}`,
`chron_rpc_duration_seconds`, `chron_streams_open`,
`chron_commits_total{db,result}`, `chron_commit_duration_seconds`,
`chron_replication_errors_total`,
`chron_replication_chunks_applied_total`); gauges are read on each scrape
(`chron_head`, `chron_floor`, `chron_segments`, `chron_segment_bytes`,
`chron_epoch`, `chron_role{role}`, `chron_replica_connected`,
`chron_leader_head`, `chron_replication_lag_events`,
`chron_fenced_by_epoch`), as are the library's commit counters
(`chron_events_committed_total`, `chron_transactions_committed_total`,
`chron_commit_batches_total`, `chron_fsyncs_total`,
`chron_fsync_seconds_total`). `chron status [--max-lag N]` is the
one-screen check with an exit code. [docs/operations.md](docs/operations.md)
holds the runbooks: promotion and failover, restoring a replica,
retention, rotating tokens, upgrading, and reading the metrics.

## Replication

A daemon started with `--replicate-from` is a **replica** of the whole
leader daemon: for every database it checks that it holds a copy of the
leader's database of the same name (same identity, a head no further
than the leader's, the same record before its head; an empty database
adopts the leader's identity), then pulls whole transactions and applies
them through the normal commit path, reconnecting with backoff when the
leader goes away. It asks the leader for its list of databases at
start, every `--discover-interval` (10 s) and whenever a puller
reconnects: a database created on the leader is created and followed on
the replica; one dropped on the leader is kept on the replica, read-only,
with `last_replication_error` saying so (remove its directory by hand).
The replication token must be the leader's admin token when it has one.
Every database of a daemon carries the same epoch; promotion raises them
all. A leader that is merely unreachable lets the replica
start disconnected; a leader that *refuses* it (another database, a
diverged history) fails the start. A replica serves reads,
subscriptions, backups and retention; it refuses writes with "read-only
replica". It keeps a `replica` marker in its data directory, and refuses
to start as a leader until promoted. `chron info` on either side shows
the role, the epoch, the identity, and on a replica whether it is
connected, the leader's head and the last error. Chunks are capped at
half the message limit; a single transaction above the limit is reported
(`RESOURCE_EXHAUSTED`, "raise --max-message-bytes") rather than retried
silently. Towards an `https://` leader the replica uses
`--replicate-ca`, `--replicate-cert` / `--replicate-key` and
`--replicate-domain`; the replication token must be the leader's admin
token when it has one.

Promotion is manual: `chron --addr <replica> promote` stops the pull,
raises the **epoch** by one, records a `promoted` marker in the data
directory and asks the old leader to fence itself; `chrond --promote` on
a stopped replica's directory does the same without a leader to tell. A
fenced leader refuses writes ("a newer leader exists"), ends its
subscriptions and replication streams with `UNAVAILABLE`, and keeps a
`fenced` marker across restarts. Writers that want the guarantee send
the epoch they last saw: `chron append --epoch N` (or
`Client::commit_with_epoch`); a stale epoch is refused, an epoch exactly
one above the daemon's fences it on the spot, and a larger jump is
refused without effect. To bring the old leader back, restart it with
`--replicate-from` the new leader: if its history still matches it
becomes a replica and the marker goes; if it took writes of its own it
is refused as diverged and must be restored from a backup of the
leader. A promoted directory refuses to become a replica until its
`promoted` marker is removed. A replica restored from a backup of a
leader retained up to its head starts at the leader's floor, where the
record before it is gone; the leader accepts it without that check.

## Known limitations

- Forward pages (iterators, subscriptions, the daemon's streams) are
  bounded by `Options::page_max_bytes` (4 MiB) as well as by records;
  backward pages are bounded by records only.
- An incremental apply or replication chunk interrupted mid-way (a crash
  between the segment write and the index commit) is rolled back on the
  next open; the caller retries from the head the error reports.
- Per-database options (fsync, record limit) are daemon-wide, and
  `default` cannot be dropped.
- Automatic failover and more than one leader are not in this version.

## Development

The gRPC code generated from `crates/chron-proto/proto/chron/v1/chron.proto`
is committed under `crates/chron-proto/src/generated/`, so building needs
no `protoc`. After editing the `.proto`, regenerate with `protoc` installed
(`brew install protobuf` or `apt-get install protobuf-compiler`):

```
CHRON_PROTO_REGEN=1 cargo build -p chron-proto
```

CI fails when the committed file does not match the `.proto`.

```
cargo fmt --all --check
cargo clippy -p chron-db --all-targets -- -D warnings                 # the library without features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --no-fail-fast
cargo doc --workspace --no-deps                                       # warning-free; the library denies missing docs
cargo bench -p chron-db   # criterion: append in both fsync modes, the three reads
cargo run -p bank -- open alice   # the example: deposit <id> 50, withdraw, balances [--follow], history <id>; --addr for a daemon
```

`cargo run -p chron-load -- --addr http://127.0.0.1:4242 --replica
http://127.0.0.1:4243 --duration 600 --backup-dir ./backups --backup-every
60 --retain-every 300` loads a leader and a replica while checking
invariants (dense positions, payloads matching their records, aggregate
lengths, restored backups, retention floors, replica equality at the
end); it exits 1 on any violation and prints counts and commit latency
percentiles (`--json` for a machine-readable report).

CI runs the first four on every push and pull request, then
`cargo package --workspace --locked` so the crates are known to package.
The suite includes property tests against a model of the semantics, a
crash test that kills a child process mid-commit and reopens,
injected-failure tests for a commit that fails after writing, TLS and
token tests against generated certificates, and replication tests with
two daemons.

### Releasing

The workspace's crates share one version. Write the release notes under
`## Unreleased` in `CHANGELOG.md`, then:

```
scripts/release.sh 0.2.0            # bumps, moves the changelog, runs the gate, commits, tags
git push origin main && git push origin v0.2.0
```

`main` goes first so the tag's workflow finds its commit there. The tag
triggers `.github/workflows/release.yml`: it checks the tag against the
version, runs the gate, builds `chrond` and `chron` for the three targets,
publishes the five crates to crates.io in dependency order (`cargo
publish --workspace`, with the repository secret `CARGO_REGISTRY_TOKEN`:
a crates.io token allowed to publish new and update crates, set once with
`gh secret set CARGO_REGISTRY_TOKEN`), and creates the GitHub Release
with the tarballs, `SHA256SUMS` and the changelog section as notes.
