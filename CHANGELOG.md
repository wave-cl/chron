# Changelog

All notable changes to chron. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the workspace's
crates share one version.

## Unreleased

### chron-db

- Streams: `Db::create_stream(name, tags)` defines a named subset of the
  global order, the events carrying any tag of the set, committed before
  or after; `read_stream`, `read_stream_bounded(_upto)`,
  `read_stream_backward(_upto)`, `iter_stream`, `subscribe_stream`,
  `stream`, `streams`, `delete_stream`; `StreamInfo`; errors `NoStream`,
  `StreamExists`, `StreamNotReady`, `InvalidStream`; `Info.streams`. The
  index gains `stream` and `stream_pos` tables (an older index reads as
  having no streams); members are indexed as records land and, at
  creation, from the tag entries a chunk at a time without holding up
  commits. A truncation drops a stream's entries from the cut, retention
  those below the floor; both by range, no record scan.
  Definitions survive a crash (the replay at open indexes for them; a
  creation interrupted before its fill finished is removed with its
  entries), an index rebuild (entries recomputed) and a full backup: an
  archive holding a stream says `format` 3 in its header and a restore
  rebuilds the entries from the definitions.

### chron-proto

- `StreamDef`, `StreamInfo`; `CreateStream`, `ListStreams`,
  `DeleteStream`, `ReadStream` (stream Event); `SubscribeRequest.stream`;
  `ReplicationChunk.streams` and `all_streams`; `InfoResponse.streams`.

### chrond

- Streams: `CreateStream` (admin, leader only) defines a named subset
  of the global order over a set of tags and returns once the earlier
  events are indexed; `ReadStream`, `Subscribe` with `stream`, cursors
  on a stream; `ListStreams`; `DeleteStream` (admin, leader only) ends
  the stream's subscriptions UNAVAILABLE. Definitions ride on the
  replication chunks, applied before the events: a follower fills a new
  stream from what it holds off its apply path, takes the leader's
  complete list on the first chunk of a Follow (dropping any stream the
  leader lacks, replacing one defined with other tags), so every node
  holds the same streams. A restore from a fetched backup keeps them.
  Metric `chron_streams{db}`; `Info.streams`.
- Gateway: `GET|POST /v1/db/{db}/streams`, `DELETE
  /v1/db/{db}/streams/{stream}`, `GET /v1/db/{db}/streams/{stream}/
  events`, `tail?stream=`; `/info` answers `streams`.

### chron-client

- `StreamInfo`, `Source`, `StreamChange`; `Client::create_stream`,
  `streams`, `delete_stream`, `read_stream(_with/_backward/_stream)`,
  `subscribe_stream`, `subscribe_source`, `subscribe_cursor_source`;
  `ReplicationChunk.streams` and `all_streams`; `Info.streams`.

### chron-cli

- `chron streams [list | create NAME --tag T... | delete NAME]`, `chron
  stream NAME [range flags]`, `chron tail --stream NAME` (with
  `--cursor` too).

## 0.6.0 - 2026-10-23

### chron-db

- Capped reads: `read_all_bounded_upto`, `read_aggregate_bounded_upto`,
  `read_tag_bounded_upto` and the `_backward_upto` forms serve only
  positions below a caller's cap, applied inside the index snapshot;
  `Subscription::next_async_upto(cap)` delivers below the cap and
  answers `None` once nothing more lies under it, resuming where it
  stopped when the cap rises. The daemon's confirmed reads are built on
  them. Existing reads are unchanged.
- `Transaction::guard(f)`: a check run under the writer lock right before
  the batch is written; a failing guard refuses that transaction alone
  with the new `Error::Refused`, the rest of the batch lands.
- The last landed batches stay in memory (four batches' worth, a
  mebibyte at least), and a replication chunk cut from them reads no
  segment file (under `F_FULLFSYNC` a read waited for the next batch's
  flush: p90 4 ms on the leader under load). A truncation empties the
  ring. `Stats::chunk_ring_hits` and `chunk_ring_misses`.
- Durable cursors: `Db::set_cursor`, `cursor`, `remove_cursor`,
  `cursors` keep a named consumer position (`next`, `session`) in the
  index (a new `cursor` table; an older index reads as empty). A
  truncation clamps cursors to the cut, a rebuilt index keeps them, a
  full backup carries them (the header says `format` 2 when the archive
  holds one; without cursors it keeps format 1) and a restore keeps
  them, clamped to the archived head.

### chron-proto

- `DatabaseOptions`, `CreateDatabaseRequest.options`,
  `DatabaseInfo.options`, `Tombstone`, `ListDatabasesResponse.dropped`.
- `CursorState`, `SubscribeRequest.cursor`, `ReplicationChunk.cursors`
  and `all_cursors`,
  `Ack`, `ListCursors`, `DeleteCursor`; headers `chron-session` and
  `chron-cursor-from`.
- `Consistency` (`LOCAL`, `CONFIRMED`, unspecified for the daemon's
  default); `consistency`, `at_least` and `wait_timeout_ms` on the read
  requests and `HeadRequest`; `HeadResponse { head, confirmed, local }`;
  `SubscribeRequest.consistency`; `ReplicationChunk.confirmed` and
  `HeartbeatRequest.confirmed` (per database); `InfoResponse.confirmed`
  and `read_consistency`.

### chrond

- `AggregateLen` takes the consistency too (and `at_least`): under
  confirmed, the aggregate's events below the confirmed bound.
- A follower takes the leader's complete cursor list on the opening
  chunk and drops any cursor it holds that the leader lacks: a delete it
  missed while away, or one restored from an older backup.
- Confirmed reads: on a node with peers a read, head or subscription
  serves, by default under sync commits, only positions a majority of
  the group durably holds (the bound rises with the followers' reports
  on a leader and arrives with chunks and heartbeats on a follower; a
  promotion keeps it); `local` serves everything the node holds;
  `--read-consistency` sets the default. `at_least` waits for the bound
  to reach a position (a commit's `next`), at most `wait_timeout_ms`,
  then answers DEADLINE_EXCEEDED with `chron-position` and `chron-head`
  (504 on the gateway). The gateway takes `consistency`, `at_least` and
  `wait_ms` on the read, head and tail routes; `/head` answers `head`,
  `confirmed` and `local`; `/info` adds `confirmed` and
  `read_consistency`.
- Per-database options: `CreateDatabase` takes `fsync` and
  `max_record_bytes` (`chron db create --fsync|--no-fsync
  --max-record-bytes`, the gateway body), kept in
  `<data_dir>/<name>.options` and reported in `DatabaseInfo.options`;
  replicas create their mirrors with the leader's. A follower counts as
  durable by the database's own fsync setting.
- Tombstones: a dropped database is remembered by identity for 30 days
  (`<data_dir>/dropped`, `ListDatabasesResponse.dropped`), replicas
  remove their copy of it (and keep the tombstone through a promotion);
  a drop a crash interrupted is finished at the next start.
- A commit checks the leader's lease again under the writer lock right
  before its batch is written: one that waited in the queue past the
  lease is refused (FAILED_PRECONDITION, nothing written) instead of
  landing under a stale lease; one whose lease ran out during the write
  itself is answered "unconfirmed". `chron_chunk_ring_hits_total` and
  `chron_chunk_ring_misses_total`.
- Durable cursors: `Subscribe` with `cursor` keeps the position on the
  leader under a session, starts at the stored position (the floor when
  below it), delivers confirmed events only and takes the cursor over
  from an older subscription; `Ack` moves it (no-op below, refused past
  the confirmed bound or from a taken-over session); `ListCursors`,
  `DeleteCursor` (admin). Cursors replicate on the chunks, so the next
  leader resumes them. Gateway `GET /v1/db/{db}/cursors`, `DELETE
  /v1/db/{db}/cursors/{name}`, `POST /v1/db/{db}/cursors/{name}/ack`,
  `/tail?cursor=`. Metrics `chron_cursors`, `chron_cursor_lag_events`,
  `chron_cursor_acks_total`.

### chron-client

- `DatabaseOptions`, `Client::create_database_with`,
  `list_databases_full` → `Listing { databases, dropped }`, `Tombstone`;
  `DatabaseInfo.fsync` and `max_record_bytes`.
- `Client::subscribe_cursor` → `(EventStream, Cursor)`, `Cursor::ack`,
  `cursors`, `delete_cursor`, `ClientError::TakenOver`;
  `ReplicationChunk.cursors` and `all_cursors`.
- `Consistency`, `ClientConfig::consistency`, `ReadOptions`, the `_with`
  reads, `head_with` (`Head { head, confirmed, local }`),
  `subscribe_with`, `ClientError::BoundTimeout`; `Info.confirmed` and
  `read_consistency`. `head()` now answers the bound under the client's
  consistency.

### chron-cli

- `db create NAME [--fsync | --no-fsync] [--max-record-bytes N]`; `db
  list` shows each database's options and the recent drops.
- `tail --cursor NAME [--ack-every N]`, `chron cursors [list | delete
  NAME]`.
- `--local` / `--confirmed` on every command, `--at-least P [--wait
  SECS]` on `all`, `read`, `tag` and `head`; `head --json` carries both
  bounds.

### tools

- `chron-load --local` reads and subscribes under the local consistency.
- `chron-load`'s subscribers remember what they saw and report
  `rewritten` (a position delivered twice with different payloads, or
  seen and then changed or missing in the final log: a confirmed event
  erased, always a violation), `seen_events` and `cut_backs` (streams
  that ended "cut back" across a failover and resumed from the cut:
  expected, never a violation). The kill run asserts `rewritten == 0`,
  and a run that removes the leader under load ends `ok`.

### examples

- The bank's `balances --follow` runs on the durable cursor `balances`
  against a daemon (`Store::subscribe_cursor`,
  `Balances::follow_durable`); `apply` skips an event below the
  checkpoint, so a redelivery changes nothing.

## 0.5.0 - 2026-10-19

### chron-db

- One fsync per batch: the index commit is made durable every
  `Options::index_checkpoint_batches` (256) or
  `index_checkpoint_interval` (1 s), on close and on every maintenance
  write, instead of with every batch; on open, complete transactions the
  segments hold past the durable index are replayed into it (across
  segment rolls; a torn trailing transaction is cut), and `truncate_to`
  records its intent before cutting so a crash in between is finished
  on open. A transaction that was written and synced but whose commit
  result never reached its committer is now kept rather than cut.
  Replication chunks and applied backups land as one batch (one segment
  sync, one index commit) instead of one per transaction. `Stats` gains
  `index_checkpoints` and `fsync_buckets`; `FSYNC_BUCKET_BOUNDS`.
- Idempotent commits: `Transaction::commit_id([u8; 16])` stores the id
  in the transaction's first record (`FLAG_COMMIT_ID`, 16 bytes after
  `recorded_at`; `Event::commit_id`) and in a new index table `txn_id`.
  A later commit with the same id and the same events' aggregates is
  answered with the original positions and `Committed::duplicate`,
  nothing written, whatever its expectations and payloads; another
  shape is `Error::CommitIdReused`. `Db::lookup_commit`,
  `Stats::duplicates`. Truncation and retention forget the ids of the
  records they drop (a resend of a transaction below the floor lands
  again); rebuild, restore and applied backups keep them. **On disk,
  one way**: a record carrying an id is refused by 0.4.0 ("unknown flag
  bits 0x08"); an index written by 0.5.0 opens on 0.5.0 without
  migration.

### chron-proto

- `Members`, `GetMembers`, `ChangeMembers`; `members` on
  `HeartbeatRequest`/`Response` and `RequestVoteRequest`/`Response`;
  `InfoResponse` 24–27 (`members_version`, `members_epoch`,
  `members_committed`, `member`), `DiscoverResponse` 6–7,
  `ROLE_REMOVED`.
- `CommitRequest.commit_id` (4), `CommitResponse.duplicate` (5),
  `Event.commit_id` (10).

### chrond

- Dynamic membership: the failover group's membership lives in
  `<data_dir>/members` on every node (generations `(epoch, version)`
  with the full set of addresses) and changes at runtime through the
  leader (`ChangeMembers`, `chron members add|remove`, `POST
  /v1/members`), one address at a time, committed once a majority of
  the new membership has persisted it and holds everything the leader
  had when the change was made. Memberships ride on heartbeats and vote
  requests: a node behind adopts, one ahead refuses; a heartbeat or vote
  request from an address outside the membership is refused (an admin
  token alone no longer lets a stranger campaign); a new leader
  republishes the membership under its epoch before taking changes. A
  node outside the membership runs **removed** (reads only, follows the
  leader it finds through its seeds, never campaigns). `--peer` seeds a
  new group and is ignored, with a warning, once the file exists; a
  directory holding one refuses to start without `--peer`. `GetMembers`
  (open), `GET /v1/members`, `members_version` on `/v1/status`; metrics
  `chron_members`, `chron_members_version`, `chron_members_epoch`,
  `chron_members_committed`, `chron_member`,
  `chron_member_changes_total`; `chron_peers` counts the other members.
- `--group-commit-max-txns`, `--group-commit-max-bytes`,
  `--index-checkpoint-batches` (env `CHROND_…`); the `chron_fsync_seconds`
  histogram and `chron_index_checkpoints_total`; commit and sync-wait
  histograms bucketed from 100 µs.
- Commit ids over gRPC and the gateway (`commit_id` in the transaction
  JSON, `duplicate` in the answer, `commit_id` on events); a reused id
  with another shape is INVALID_ARGUMENT / 400;
  `chron_commits_total{result="duplicate"}`.
- A follower lands the chunks that arrived while it was applying the
  previous one as one batch (one segment sync, one index commit, one
  report), instead of one batch per leader batch. The commit and
  replication phases are traced under `chrond::timing` at `trace`
  level, and `docs/operations.md` has the measured breakdown of a
  three-node sync commit.

### chron-client

- `Role::Removed`, `Members`, `MembersStatus`, `Client::members`,
  `add_member`, `remove_member`; `Info` and `Discovered` carry the
  membership generation; `connect_any` also asks the members a seed
  names.
- Every commit carries a random commit id (`ClientConfig::commit_ids`,
  default on; `CommitOptions::commit_id` to choose it) and is therefore
  resent after `Unavailable`, `Transport`, `Fenced`, `Unconfirmed` and a
  deadline, free appends and epoch tokens included, landing exactly
  once; `commit_ids: false` keeps the 0.4.0 rule. Upgrade daemons before
  clients: a 0.4.0 daemon ignores the id.

### chron-cli

- `chron members [list | add ADDR [--force] | remove ADDR]`; `status`
  is degraded on a removed node and shows the membership; `info` and
  `discover` show its version.
- `append --commit-id`, `commit --commit-id` (or `"commit_id"` in the
  file); a resend prints `already committed` (`"duplicate"` in JSON);
  events show their `commit_id`.

### tools

- `chron-load --in-flight N` keeps N commits per writer in flight on
  distinct aggregates; the report carries `wall_s`,
  `transactions_per_s`, `events_per_s`, `writers`, `in_flight` and the
  leader's `fsync`; its tests take `CHRON_LOAD_FSYNC=1` and
  `CHRON_LOAD_IN_FLIGHT=N`. `scripts/e2e-load.sh` runs release daemons
  under it and prints throughput, p50, p99 and fsyncs per transaction;
  CI's new `bench` job runs the criterion benches and that load and
  writes the numbers to the run's summary.
- `chron-load` counts `duplicates`.

## 0.4.0 - 2026-10-19

### chron-proto

- `Follow`: replication with acknowledgments (a `FollowRequest` stream
  of `open` then `applied` messages, chunks back); `CommitRequest.sync`;
  `ReplicationChunk.epoch_base`; `InfoResponse` gains
  `majority_applied`, `followers` and `sync_commit`; the metadata keys
  `chron-committed-first` and `chron-committed-next` on a sync commit
  answered `DEADLINE_EXCEEDED`.

### chrond

- `FetchBackup`: a full backup streamed as the archive's bytes, from any
  node, needing no `--backup-dir` (admin). A replica with peers whose
  head or shared history lies below the leader's floor restores itself
  from it: the copy is restored at `<data_dir>/<db>.fetched`, checked,
  swapped in, the old directory kept as `<db>.replaced-<nanos>`, and an
  interrupted swap is finished or undone at the next start. Streams on
  the replaced database end with "replaced by a copy restored from the
  leader". `chron_backup_fetches_total{db,result}` and
  `chron_restores_total{db,result}`. The below-floor refusal of
  `Replicate` now carries the floor metadata.
- Synchronous replication: with peers a commit is acknowledged only once
  a majority durably holds it (`--sync-commit majority`, the default;
  `off` for the old behaviour; `--sync-timeout`, the lease ttl by
  default). Followers report applied heads through `Follow` and fall
  back to `Replicate` on a leader without it. A commit not confirmed in
  time, or before the node stopped leading, answers `DEADLINE_EXCEEDED`
  "unconfirmed" with the positions it landed at. Only configured peers
  at least as durable as the leader count. A follower adopts the
  leader's epoch only once it holds what that epoch's leader held at its
  election, and a candidate claims the lowest epoch among its databases,
  so an elected leader holds every confirmed write; with peers a start
  no longer raises a lagging database's epoch. Metrics
  `chron_sync_commit_wait_seconds`, `chron_sync_commit_timeouts_total`,
  `chron_majority_applied`, `chron_follower_applied_head`,
  `chron_follower_counted`, `chron_sync_commit`; `commit_duration`
  includes the wait; `chron_commits_total{result="unconfirmed"}`. The
  gateway takes `?sync=` and answers 504 with `committed_first` and
  `committed_next`.

- Failover review fixes: a campaign asks for pre-votes first, so a lost
  or refused campaign (a forced promotion of a lagging node included)
  leaves no promise on any node; a forced vote is exempt from the
  recency rule only on the leader itself, so a node that cannot reach
  the live leader cannot be promoted over it; a follower waits out the
  longer of its own lease ttl and its leader's; a candidate re-checks
  under the transition lock that it heard no leader; up-to-dateness is
  a total order (epoch, per-database heads, then the larger total and
  the lower address), so an election always ends; a fenced node
  campaigns after a lease of silence; a node waits a lease between
  campaigns; with peers a client's epoch token no longer fences the
  leader; shutdown refuses cluster calls and stops the cluster task
  before the pullers; an unreadable `vote` marker stops the start;
  election jitter is seeded from the wall clock.
- Resync fixes: the backup, cut and reset notice run as one task that
  always finishes, the archive is never overwritten (a second cut at the
  same positions gets a timestamp), the head is re-checked before the
  cut, fingerprints are compared on the blocking pool with a bound on
  the leader's answer, and the boundary walk reads backward in pages.
  A replica whose index has no record below its head is refused rather
  than fed a hole.
- Gateway: `limit=0` means the maximum page, `expect` accepts a number
  in quotes, binary tags via `tag_base64`, the body limit allows for
  base64, 409 errors carry `reason` (`expectation` or `epoch`), the
  documented outcome of a 408, and subscriptions end on a cut before
  any buffered event.

### chron-db

- `Db::backup_to(&mut impl Write)` and `restore_backup_from(impl Read,
  dir, opts)`: the archive to and from any sink or source, without a
  file in between; the path-based calls are unchanged.
- `Db::truncate_to` publishes the new head and reopens the writer right
  after the index commit, so a later file error leaves a consistent
  database; a `Subscription` that saw the old tail reports a cut
  (`Error::Inconsistent`) instead of skipping positions.

### chron-client

- `Client::fetch_backup` (a byte stream) and `fetch_backup_to(path)`
  (written through a `.tmp` file, fsynced, renamed; returns the header);
  `ClientError::Io`.
- `CommitOptions` and `Client::commit_with` (an epoch token and a sync
  choice), `ClientConfig::sync_commit`, `ClientError::Unconfirmed`
  (never retried), the `Info` fields `majority_applied`, `followers` and
  `sync_commit`, `FollowerStatus`, and the daemon-to-daemon `follow`.
- `connect_any` uses a seed that leads itself and otherwise believes a
  named leader only once it confirms it leads, with bounded probes and
  one search at a time; `ClientConfig::failover_timeout` (30 s)
  replaces `failover_retries`; a commit is resent after a connection
  failure only when every append has an exact or `new` expectation
  (never with an epoch token), and `commit` documents the rule;
  transport errors show their cause; a daemon killed under an open call
  (an `Unknown` status with the h2 body error) is `Unavailable` like a
  daemon shut down under one.

### chron-cli

- `chron backup --fetch FILE` downloads a full backup to a local file
  from any node; `chron restore` takes it unchanged.
- `chron append` and `chron commit` take `--sync` or `--async`; an
  unconfirmed commit prints the positions it landed at; `chron info`
  shows the sync default, the majority-applied position and one line per
  follower.
- `chron discover` asks every address of `--addr` and prints one line
  per node; a list with mixed schemes is a usage error; "no leader"
  exits 2. The JSON `commit` path refuses an expectation on a free
  append, as the flag path does.

### tools

- `chron-load --sync` / `--async`; the report counts `unconfirmed`
  commits; the kill test runs in both modes and asserts that no
  acknowledged write is lost in sync mode.
- `chron-load` kill mode: a kill is counted only when it happened, waits
  until every other node is a connected follower, keeps the leader's
  address as the `--pid` key, treats only not-the-leader, unreachable
  and timed-out calls as failover errors, keeps every violation, counts
  errors as failures, and checks acknowledgments against the log without
  holding the whole log in memory.

## 0.3.0 - 2026-10-18

### chron-db

- `Db::truncate_to(position)`: drops the records from a transaction
  boundary to the head (`Truncated`), so a copy that took writes of its
  own can cut back to the history it shares with another copy.

### chrond

- An HTTP/JSON gateway on `--http-listen` (`CHROND_HTTP_LISTEN`): the
  CLI's JSON shapes over HTTP/1.1, `tail` as server-sent events,
  `/openapi.json`, the daemon's tokens and TLS, errors with the gRPC
  code's name and metadata, `chron_http_requests_total{route,status}`.
- Automatic failover: daemons started with each other as `--peer`s elect
  a leader by majority and replace it when it dies. The leader holds a
  majority lease renewed by heartbeats (`--lease-ttl`, 10 s) and refuses
  writes the instant it expires; followers campaign after a lease of
  silence; votes persist in a `vote` marker; a node never starts as a
  leader with peers (`ROLE_CANDIDATE`). `Heartbeat`, `RequestVote` and
  `Discover` RPCs; `Info` gains `leader_addr`, `lease_remaining_ms`,
  `peers`, `voted_epoch`; `Promote` with peers is a forced campaign that
  refuses a lagging node; `--advertise`. Metrics
  `chron_lease_remaining_seconds`, `chron_leader_known`,
  `chron_voted_epoch`, `chron_peers`, `chron_elections_total`,
  `chron_leader_changes_total`, `chron_heartbeat_failures_total{peer}`.
  Writes acknowledged by a dead leader after the last transaction its
  successor had pulled are lost; see the README.
- A diverged replica with peers resets itself: it finds the shared
  history through the new `Fingerprints` RPC, keeps the tail it drops as
  `diverged-<epoch>-<from>-<head>.chronbak` in the database directory,
  cuts back and follows. Subscriptions past the cut end with
  `UNAVAILABLE` ("cut back").

### chron-client

- `Client::connect_any(seeds, config)` finds the leader of a failover
  group from any seed; commits refused as not-the-leader or failing to
  reach it look for the leader again (`ClientConfig::failover_retries`).
  `Client::discover`, `Client::rediscover`, `Client::addr`,
  `Client::fingerprints`, `Role::Candidate`, `Discovered`,
  `ClientError::NoLeader`, the new `Info` fields.

### chron-cli

- `chron append` takes several `-d` payloads (one event each, one
  transaction) and reads one event per line from `--stdin`.
- `--addr` takes a comma list of a failover group's nodes; `chron
  discover`; `info` and `status` show the leader, lease, peers and vote;
  `status` is degraded on a candidate or an expired lease.

### tools

- `chron-load` (not published): a load generator that checks invariants
  while it runs (dense positions, payload/record agreement, lengths,
  restored backups, retention floors, a replica compared at the end).
  `--kill-leader-every` with `--pid ADDR=PID` kills the leader of a
  group on a timer and checks that no write acknowledged by a surviving
  leader is lost.

## 0.2.0 - 2026-10-18

### chron-db

- Group commit: transactions committed concurrently are landed together
  (one segment write, one fsync, one index commit for up to
  `Options::group_commit_max_txns`), each still checked and acknowledged
  on its own. `Db::stats()` reports batches, transactions, events,
  fsyncs and fsync time. Eight threads committing under `Fsync::Always`
  went from about 10 ms to 4 ms per transaction on a laptop.
- `read_all_bounded`, `read_aggregate_bounded`, `read_tag_bounded`:
  forward pages bounded in bytes; the iterators, subscriptions and the
  daemon's forward streams use them with `Options::page_max_bytes`
  (4 MiB).
- Open segment files are bounded by `Options::max_open_segments` (64),
  least recently used closed first.

### chrond, chron-proto, chron-client, chron-cli

- Several databases per daemon. A call names its database with the
  metadata `chron-db: <name>`; absent means `default`, so 0.1.0 clients
  keep working. New RPCs CreateDatabase, ListDatabases and DropDatabase
  (admin; create and drop on the leader only); `Info.database`. A replica
  mirrors every database of its leader and discovers new ones
  (`--discover-interval`). Promotion raises every database's epoch.
- Layout: `<data_dir>/<name>/` per database; a 0.1.0 data directory is
  moved into `default/` on the first start (`Info.path` changes
  accordingly). Backups over the daemon now live under
  `<backup_dir>/<name>/`, so `default`'s move from `<backup_dir>/` to
  `<backup_dir>/default/`.
- Client: `ClientConfig.database`, `Client::database(name)`,
  `create_database`, `list_databases`, `drop_database`, `DatabaseInfo`,
  `ClientError::{NotFound, AlreadyExists}`. CLI: `--db` (`CHRON_DB`),
  `chron db create|list|drop`.
- Metrics and operations: `chrond --metrics-listen ADDR` serves
  Prometheus metrics at `GET /metrics` (calls by method and code, commit
  results and durations, open streams, per-database head/floor/epoch/
  role, replication lag and connection, the library's commit counters);
  `--log-format json`; `chron status [--max-lag N]` with exit codes;
  runbooks in `docs/operations.md`.

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
