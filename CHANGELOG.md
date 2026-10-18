# Changelog

All notable changes to chron. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the workspace's
crates share one version.

## Unreleased

Nothing yet.

## 0.1.0 - 2026-10-18

The first release: an embeddable event-sourcing library, a gRPC daemon
with replication, a typed client and a CLI.

### chron-db (the library, `use chron::`)

- Aggregates keyed by binary UUID; transactions appending to any number
  of them atomically, each append guarded by `Expected::{Any, New,
  Exact(n)}`; a failed expectation refuses the whole transaction.
- One global order with dense positions; tags, any number per event,
  readable as an ordered subset; free events that belong to no aggregate.
- Reads forward and backward (`limit` 0 for everything), paging
  iterators, live subscriptions with a timeout and an async variant
  (feature `tokio`).
- Segment files plus a redb index; the index commit is the commit point;
  recovery truncates unacknowledged bytes; `verify_all_segments`;
  `rebuild_index` from the segments alone.
- Full and incremental backups with CRC trailers, restore, apply and
  inspect; retention by whole segments (`retain_from`).
- A database identity and an epoch, kept across backups and rebuilds;
  replication chunks and `apply_chunk` for a copy to follow a source.
- Every error variant and field documented; `deny(missing_docs)`.

### chron-proto

- The `chron.v1` gRPC contract and its generated code, committed so
  consumers need no `protoc`; feature `convert` for the library's types.

### chron-client

- A typed async client speaking the library's types: library-shaped
  reads with streaming variants, subscriptions, admin calls, typed errors
  (`Conflict`, `Fenced`, `BelowFloor`, `PositionOutOfRange`, ...), TLS
  and mTLS, token and admin token, request and connect timeouts.

### chrond

- One database behind `chron.v1.Chron`: finite reads streamed under
  back-pressure, subscriptions, backups by name under `--backup-dir`,
  retention, health (`chron.v1.Chron.leader` only while leading).
- TLS and mTLS, a bearer token on every call, a separate admin token for
  the administrative calls, message-size limits, keepalives, a request
  timeout, one log line per call.
- Replication: a replica follows a leader by pulling whole transactions,
  reconnecting with backoff; manual promotion with epochs and fencing
  tokens; `promoted`, `fenced` and `replica` markers across restarts;
  `--promote` at start.

### chron-cli

- `chron`: append, commit from JSON, read, all, tag, tail, len, head,
  info, flush, backup, apply, retain, health, promote, fence, and the
  offline rebuild-index, verify, restore and inspect; `--json` output;
  exit codes 0, 1 (refused) and 2 (usage or unreachable).

### examples/bank

- Accounts as aggregates with optimistic commands and a persisted
  balances projection, embedded or through a daemon.
