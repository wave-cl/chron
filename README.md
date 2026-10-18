# chron

An event-sourcing database: a Rust library, a gRPC daemon and a CLI.

| Crate | What |
|---|---|
| `chron` | the embeddable library (this README's first half) |
| `chron-proto` | the `chron.v1` gRPC contract and generated code |
| `chrond` | the daemon: one database behind that service, as a leader or a replica |
| `chron-cli` | the `chron` binary: client and offline maintenance |
| `chron-client` | a typed async client in the library's vocabulary |
| `examples/bank` | accounts as aggregates, embedded or through a daemon |

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
let order = Uuid::new_v4();     // `uuid` with the `v4` feature; any Uuid works
let customer = Uuid::new_v4();

let mut txn = db.transaction();
txn.append_one(order, Expected::New, NewEvent::new(r#"{"placed":true}"#).tag("orders"));
txn.append_one(customer, Expected::Exact(3), NewEvent::new(r#"{"ordered":1}"#).tag("customers"));
txn.append_free(vec![NewEvent::new("audit: order placed").tag("audit")]);
let committed = txn.commit()?;          // Err(Error::ExpectedLength{..}) if `customer` is not at 3

let events = db.read_aggregate(order, 0, 100)?;     // from index 0
let everything = db.read_all(committed.first, 100)?; // from a global position
let audit = db.read_tag("audit", 0, 100)?;            // the tagged subset, in global order
let newest = db.read_all_backward(u64::MAX, 10)?;     // the last ten, newest first

for event in db.subscribe_tag("orders", 0) {          // every order event so far, then live
    let event = event?;
    println!("{} {:?}", event.position, event.payload);
}
```

Every read has a backward twin (`read_all_backward`,
`read_aggregate_backward`, `read_tag_backward`) that takes an inclusive
upper bound and returns newest first; `u64::MAX` means "from the end".

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
  process crash; `flush()` (also run on drop) makes it durable.
- **Recovery.** Event bytes go to append-only segment files and the indexes
  to one redb file; the redb commit is the commit point. On open, anything
  in the segments past the committed head was never acknowledged and is
  truncated. Damage inside committed data is reported as `Error::Corrupt`,
  never repaired silently.
- **One writer, many readers.** `Db` is cheap to clone and shared between
  threads; commits serialise on one lock, reads never take it. A `LOCK`
  file refuses a second process.
- **Live subscriptions.** `subscribe(from)` and `subscribe_tag(tag, from)`
  return a `Subscription`: an iterator that catches up through the index
  and then blocks for new commits. `next_timeout` serves threads that also
  watch a stop flag; `position()` is the checkpoint to resume from. A
  subscription keeps the database open.

## Layout on disk

```
<path>/
  LOCK                      held while a Db is open
  index.redb                head, aggregate lengths, (aggregate, index) → position,
                            position → (segment, offset, length), (tag, position)
  segments/<base>.seg       64-byte header, then framed records (length, crc32, body)
```

## Backups

- `db.backup(archive)` writes a **full** backup while the database stays in
  use: each segment is copied up to where the index places its last
  committed record, so the archive holds exactly the events below the head
  at that moment.
- `db.backup_since(archive, since)` writes an **incremental** backup: the
  frames of positions `since..head`, to be applied onto a database whose
  head is `since`.
- `chron::restore_backup(archive, dir, &opts)` restores a full backup into
  a directory that must not exist: the segments go to a staging directory,
  the archive's CRC is checked, the index is **rebuilt** from the
  segments, and only then is the staging directory renamed into place.
- `db.apply_backup(archive)` appends an incremental backup's events after
  checking every frame; the head must equal the archive's base head
  (`Error::WrongBaseHead`), and a damaged archive changes nothing.
- `chron::inspect_backup(archive)` reads the header only.

The archive is one file: a magic, a JSON header (`BackupMeta`: kind,
base head, head, floor, time, counts), the entries, and a CRC32 trailer.

## Retention

`db.retain_from(floor)` drops every segment that lies wholly below
`floor`. The floor in force rounds **down** to a segment boundary (the
base of the first kept segment), positions never renumber, and the tail
segment is never dropped. Reads below the floor fail with
`Error::BelowFloor`; backward reads stop at it; aggregate and tag reads
return what survives; aggregate lengths are unchanged, so
`Expected::Exact` keeps working. The index is updated first, so a crash
before the files are removed is repaired on the next open. `db.info()`
reports head, floor and the segment directory.

## Checking and repairing

- `Options::verify_all_segments(true)` reads every segment on open, checking
  each record's CRC, that positions run consecutively, and that the index
  places every record where it is. Off by default, when only the tail is
  checked.
- `Db::rebuild_index(path, &opts)` rebuilds `index.redb` from the segment
  files alone, for a missing or damaged index. It takes `LOCK` (no `Db`
  may be open), writes the new index beside the old one and renames it
  into place only when complete. Every complete transaction in the
  segments is indexed, including a trailing one whose commit was never
  acknowledged; a torn trailing transaction is dropped and the next open
  truncates its bytes. Damage anywhere else is reported, never repaired.
  A first segment above zero is read as a retention floor.

## Async

With the `tokio` feature, head changes are also published on a tokio
watch channel: `Db::wait_for_async(min_head)` and
`Subscription::next_async()`, which is cancel-safe at its one await, so
`tokio::time::timeout(d, sub.next_async())` is the timeout idiom. The
daemon uses it; the sync API is unchanged and the feature is off by
default.

## The daemon

```
cargo run -p chrond -- --data-dir ./orders.chron            # 127.0.0.1:4242
chrond --data-dir DIR --listen 0.0.0.0:4242 [--no-fsync] [--verify-all-segments]
       [--tls-cert server.pem --tls-key server.key [--tls-client-ca ca.pem]]
       [--token SECRET | --token-file FILE]
       [--max-message-bytes N] [--request-timeout SECS] [--concurrency-per-connection N]
       [--replicate-from http://leader:4242 [--replicate-token SECRET]]
```

Flags fall back to `CHRON_DATA_DIR`, `CHRON_LISTEN`, `CHROND_TLS_*`,
`CHRON_TOKEN`, `CHRON_TOKEN_FILE`, `CHRON_REPLICATE_FROM`; logging follows
`CHROND_LOG` (an `EnvFilter`, default `info`) on stderr, one line per call
with its method, gRPC status and duration. Ctrl-C or SIGTERM stops it:
every stream ends, the server drains, the database is flushed.

The service is `chron.v1.Chron` in
`crates/chron-proto/proto/chron/v1/chron.proto`: commit, the six reads
(server-streamed, `limit` 0 for everything), subscribe (never ends), head,
aggregate length, info, flush, backup, apply, retain, replicate, promote
and fence. A failed expectation is `FAILED_PRECONDITION` with the metadata
`chron-actual-length` and `chron-aggregate`; a write refused by the node's
role or epoch is `FAILED_PRECONDITION` with `chron-epoch`; bad requests are
`INVALID_ARGUMENT`; positions past the head or below the floor are
`OUT_OF_RANGE`.

Plaintext and open by default; off a loopback address, enable TLS and a
token. With `--tls-client-ca` only clients presenting a certificate from
that CA are accepted. With a token, every `chron.v1.Chron` call must
carry `authorization: Bearer <token>` (compared in constant time);
`grpc.health.v1.Health` stays open and reports SERVING for `""` and
`chron.v1.Chron` until shutdown, so `grpc_health_probe -addr host:4242`
works as is. Messages above `--max-message-bytes` (64 MiB) are refused;
the limit must cover the database's `max_record_bytes`. HTTP/2 keepalives
run every 30 s.

## Replication

A daemon started with `--replicate-from` is a **replica**: it checks that
it is a copy of the leader (same identity, a head no further than the
leader's, the same record before its head; an empty database adopts the
leader's identity), then pulls whole transactions and applies them
through the normal commit path, reconnecting with backoff when the
leader goes away. A replica serves reads, subscriptions, backups and
retention; it refuses writes with "read-only replica". `chron info` on
either side shows the role, the epoch, the identity, and on a replica
whether it is connected, the leader's head and the last error.

Promotion is manual: `chron --addr <replica> promote` stops the pull,
raises the **epoch** by one, records a `promoted` marker in the data
directory and asks the old leader to fence itself. A fenced leader
refuses writes ("a newer leader exists") and keeps a `fenced` marker
across restarts. Writers that want the guarantee send the epoch they
last saw: `chron append --epoch N` (or `Client::commit_with_epoch`); a
stale epoch is refused, and an epoch newer than the daemon's fences it
on the spot. To bring the old leader back, restart it with
`--replicate-from` the new leader: if its history still matches it
becomes a replica and the marker goes; if it took writes of its own it
is refused as diverged and must be restored from a backup of the
leader. A promoted directory refuses to become a replica until its
`promoted` marker is removed.

## The CLI

```
chron append <AGGREGATE|free> [--expect any|new|N] [--tag T]... [--meta M] (-d PAYLOAD | --stdin)
chron commit (-f FILE | --stdin)         # {"appends":[{"aggregate":"<uuid>"|null,"expect":"new","events":[{"payload":"…","tags":["…"]}]}]}
chron read <AGGREGATE> | all | tag <TAG>   [--from N] [--limit N] [--backward]
chron tail [--from N | --from-head] [--tag T]
chron len <AGGREGATE> | head | info | flush
chron backup <ARCHIVE> [--since N] | apply <ARCHIVE> | retain <FLOOR>     # paths on the daemon's filesystem
chron rebuild-index <DIR> | verify <DIR> | restore <ARCHIVE> <DIR> | inspect <ARCHIVE>   # offline, no daemon
```

```
chron health [--service NAME]            # exit 0 when SERVING, 1 otherwise
chron promote | fence <EPOCH>             # replication
chron append --epoch N … | commit --epoch N …
```

`--addr` (or `CHRON_ADDR`, default `http://127.0.0.1:4242`) names the
daemon; `https://` enables TLS with `--tls-ca`, `--tls-cert`/`--tls-key`
and `--tls-domain` (`CHRON_TLS_*`); `--token` (`CHRON_TOKEN`) sends the
bearer token. `--json` prints one JSON object per line, with `payload`
and `metadata` as strings when they are UTF-8 and as `payload_base64` /
`metadata_base64` otherwise. Exit codes: 0 ok, 1 the daemon refused or
an offline command failed, 2 usage or an unreachable daemon.

## The client crate

```rust
use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};

let client = Client::connect("http://127.0.0.1:4242", ClientConfig::default()).await?;
let order = Uuid::new_v4();
let mut txn = Transaction::new();
txn.append_one(order, Expected::New, NewEvent::new("placed").tag("orders"));
let committed = client.commit(txn).await?;            // Err(ClientError::Conflict { actual, .. }) on a failed expectation
let events = client.read_aggregate(order, 0, 0, false).await?;
let mut live = client.subscribe(committed.next, Some("orders".into())).await?;
```

`chron-client` speaks the library's types, so code written against an
embedded `chron::Db` moves to a daemon by swapping the handle;
`examples/bank` does exactly that behind a small `Store` trait
(`cargo run -p bank -- open alice`, `deposit <id> 50`, `withdraw`,
`balances [--follow]`, `history <id>`; `--addr` switches to a daemon).

## Limits

Tags are at most 255 bytes and 255 per event. One event's record is at most
`Options::max_record_bytes` (16 MiB by default). Segments roll at
`Options::segment_max_bytes` (256 MiB by default); a transaction is never
split across segments.

## Not in this version

A crates.io release, automatic failover, more than one leader.

## Development

`protoc` must be installed (`brew install protobuf` or
`apt-get install protobuf-compiler`).

```
cargo fmt --all --check
cargo clippy -p chron --all-targets -- -D warnings                    # the library without features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --no-fail-fast
cargo bench -p chron   # criterion: append in both fsync modes, the three reads
```

CI runs the first four on every push and pull request. The suite
includes property tests against a model of the semantics, a crash test
that kills a child process mid-commit and reopens, TLS and token tests
against generated certificates, and replication tests with two daemons.
