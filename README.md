# chron

An embeddable event-sourcing database, as a Rust library.

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

## Limits

Tags are at most 255 bytes and 255 per event. One event's record is at most
`Options::max_record_bytes` (16 MiB by default). Segments roll at
`Options::segment_max_bytes` (256 MiB by default); a transaction is never
split across segments.

## Not in this version

Backups, segment deletion, a server, a CLI.

## Development

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo nextest run --workspace --no-fail-fast
cargo bench            # criterion: append in both fsync modes, the three reads
```

CI runs the first three on every push and pull request.
