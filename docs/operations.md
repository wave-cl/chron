# Operating chron

Runbooks for a leader with replicas. `$LEADER` and `$REPLICA` are
`http://host:4242`-style addresses; the admin token travels in
`CHRON_ADMIN_TOKEN` (or `--admin-token`). `chron status` exits 0 when the
node is healthy, 1 when it is degraded (a replica disconnected, a fenced
node, a candidate, an expired lease, lag above `--max-lag`), 2 when
unreachable.

## Promotion and failover

Planned, the leader still up:

1. `chron --addr $REPLICA status --max-lag 0` — the replica is connected
   and caught up.
2. `chron --addr $REPLICA promote` — the replica leads under the next
   epoch and tells the old leader to fence itself.
3. Repoint clients to `$REPLICA`. Writers that send `--epoch` refresh it
   from `chron info`.
4. `chron --addr $LEADER info` shows `role fenced`. Stop the old daemon
   and restart it as a replica of the new leader:
   `chrond --data-dir DIR --replicate-from $REPLICA --replicate-token-file FILE`.
5. `chron --addr $OLD status --max-lag 0` once it has caught up.

Unplanned, the leader dead:

1. `chron --addr $REPLICA promote` (the fencer retries until the old
   leader answers), or on a stopped replica directory
   `chrond --promote --data-dir DIR` then start it normally.
2. Repoint clients.
3. When the old leader comes back, start it with `--replicate-from` the
   new leader. If it took writes nobody saw it is refused as diverged:
   restore it from a backup of the new leader (next runbook).

## A failover group

Three nodes that elect their own leader. `$A`, `$B`, `$C` are their
addresses as the others reach them; the replication token is the admin
token.

Starting the group:

1. On each host, the same flags but its own `--listen` and `--advertise`:
   `chrond --data-dir DIR --listen 0.0.0.0:4242 --advertise $A --peer $B --peer $C --admin-token-file FILE --replicate-token-file FILE --metrics-listen 127.0.0.1:9100`.
   Fresh directories, or one node's existing directory and empty ones
   for the others (they adopt its identity when they first follow it).
2. Within a lease (10 s) plus an election: `chron --addr $A discover`
   names the leader; `chron --addr $A,$B,$C status` is healthy on it.
3. Clients take the whole list: `--addr $A,$B,$C`, or
   `Client::connect_any`.

The leader died:

1. Nothing to do: within one to two leases the surviving pair elects the
   more up-to-date one; `chron_leader_changes_total` rises on it,
   `chron --addr $B,$C discover` names it. Writers using `connect_any`
   retry on their own for `failover_timeout` (30 s): with commit ids
   (the default since 0.5.0) every commit is resent and lands exactly
   once; with ids off a commit that may have landed is resent only with
   an exact or `new` expectation. Others repoint.
2. With `--sync-commit majority` (the default) nothing acknowledged is
   lost; a writer that received "unconfirmed" resends with the same
   commit id, or without one checks before resending (an exact
   expectation, or a read). With `off`, writes the dead leader
   acknowledged after the last transaction the new leader had pulled are
   lost: the replication lag at that moment (`chron_replication_lag_events`;
   `chron-load --kill-leader-every` measures it on your traffic).
3. Start the old node again with the same flags. It comes up as a
   candidate, hears the leader and follows it. If it took writes nobody
   replicated, it cuts itself back to the shared history (the log says
   "dropped a diverged tail") and keeps those writes in
   `<data_dir>/<db>/diverged-<epoch>-<from>-<head>.chronbak`; `chron
   restore` that file into a scratch directory to read them, and
   remove it when done.

A planned switchover to `$B`:

1. `chron --addr $B status --max-lag 0` — connected and caught up.
2. Pause writers, or accept that the step below is refused while `$B`
   lags behind the leader's head (it retries five times a second apart).
3. `chron --addr $B promote` — a forced campaign: the leader fences
   itself and grants the vote, the third node grants it, `$B` leads
   under the next epoch. The old leader follows `$B` by itself on the
   next heartbeat.
4. Resume writers; `chron --addr $A,$B,$C status` is healthy on `$B`.

A node whose shared history lies below the leader's retention floor, or
an empty or long-stopped node joining a retained group, restores itself
from the leader's backup over the wire: `chron_restores_total{result="ok"}`
rises on it, its log says "replaced by a copy restored from the leader",
and its old directory stays beside the data as
`<data_dir>/<db>.replaced-<nanos>` (the diverged case's own writes are in
there; remove it when no longer needed). What stays manual:

- an identity mismatch ("is database X; the leader serves Y"): the node
  was never a copy of this group's database; move its data directory
  aside and start it empty;
- a restore that fails for space or permissions
  (`chron_restores_total{result="error"}`, `last_error` on `chron info`):
  fix the cause; the node tries again on its own;
- a node without peers (`--replicate-from`): see the next runbook.

A leader whose lease keeps expiring (`chron_lease_remaining_seconds` at
0, `chron_heartbeat_failures_total` rising): the majority cannot be
reached within a third of the lease. Check the network between the
nodes before raising `--lease-ttl`; a longer lease is a longer outage
when a leader is cut off, and more writes on it that will be lost.

## Adding a node

1. Start it empty with any member as `--peer` and the same tokens and
   TLS: `chrond --data-dir DIR --listen 0.0.0.0:4242 --advertise $D --peer $A ...`.
   It comes up removed and follows the leader (`chron --addr $D status`
   says `removed`, connected).
2. `chron --addr $LEADER members add $D`: `committed` once a majority of
   the new membership holds it and the leader's head, else `pending`,
   then `chron --addr $LEADER members` says when. With four nodes a
   majority is three: add the node only once it is reachable, since a
   member that cannot answer costs the lease one failure sooner.
3. `chron --addr $D status --max-lag 0` shows `replica`.

One node at a time; a second change is refused while one is pending.

## Removing a node

- A follower, alive or dead: `chron --addr $LEADER members remove $X`,
  committed once a majority of the remaining nodes holds it. Stop the
  daemon, or leave it: it runs removed (reads only) until added again.
- The leader: promote another node first (`chron --addr $B promote`,
  no write pause), then remove the old leader as a follower. Removing
  the leader directly works too, but pauses writes for about a lease
  while the others elect.
- Never go below three nodes unless an outage of either remaining node
  is acceptable.
- To reuse a removed node's directory alone or as the seed of a new
  group: stop it, delete `DIR/members`, start it with the right `--peer`
  flags (or none). On one node at a time, never on two at once: two
  deleted files and matching flags form a group of their own.

## Replacing a node

Add the new node (above), wait for `status --max-lag 0` on it, then
remove the old one. The group runs with one more member in between, so
do it while the others are healthy. A node that changes address keeps
its directory: `--advertise` is its identity, so remove the old address,
start the daemon under the new one with `--peer` any member, and add the
new address.

## Restoring a replica from the leader

With peers nothing to do: a node below the leader's floor fetches the
leader's backup and restores itself (previous section). Without peers,
on the replica host:

1. `chron --addr $LEADER backup --fetch full.chronbak --db NAME` for
   every database (any node serves it; no `--backup-dir` needed; the
   leader's retention and other backups of that database wait while the
   transfer runs).
2. `chron restore full.chronbak DIR/NAME` for each, then
   `chron verify DIR/NAME`. The directory `DIR` is the data directory;
   each database is a subdirectory, `default` at least.
3. Remove any `promoted` or `fenced` marker in `DIR`.
4. `chrond --data-dir DIR --replicate-from $LEADER --replicate-token-file FILE`.
5. Watch `chron_replication_lag_events` reach 0, or
   `chron --addr $REPLICA status --max-lag 0`.

## Retention

1. Take a full backup first (above).
2. Every replica should be caught up (`status --max-lag 0` on each): a
   replica below the leader's floor cannot resume; with peers it
   restores itself from the leader over the wire, without peers an
   operator does (two runbooks above).
3. `chron --addr $LEADER retain FLOOR`, then the same on each replica.
4. `chron info` (or `chron_floor`) confirms the floor, which lands on a
   segment boundary at or below FLOOR. Commit ids of transactions below
   the floor are forgotten with them: a client resending one that old
   lands it again.

## Rotating tokens

There is no two-token overlap: a rotation is a restart. Run every daemon
with `--token-file`, `--admin-token-file` and `--replicate-token-file`
so only file contents change.

1. Write the new tokens to the files on every host.
2. Restart the leader. Clients see `UNAUTHENTICATED` until they carry the
   new token; replicas see `UNAVAILABLE` or `UNAUTHENTICATED` until step 3.
3. Restart each replica.
4. `chron_replica_connected` is back to 1 on every replica and
   `chron_replication_errors_total` stops rising.

## Sync commits time out

`chron_sync_commit_timeouts_total` rising, `chron_commits_total
{result="unconfirmed"}` on the leader, writers seeing "unconfirmed".

1. `chron --addr $LEADER info`: `majority_applied` against `head`, and
   one `follower` line each: which peer lags (`applied`), which is
   missing (not attached: check `chron status` there), which is
   `counted false` (a `--no-fsync` follower of an fsync leader, or a
   node outside the membership), which runs a version without `Follow`.
2. `chron_follower_applied_head{peer}` over time shows a slow follower;
   `chron_replica_connected` on it shows a broken link.
3. The escape hatch while fixing it: `--sync-commit off` on the leader,
   or `--async` per request, with the loss window that implies.

## Upgrading

1. Read the CHANGELOG for wire or disk changes.
2. Followers first: restart each, then `status --max-lag 0`. A leader on
   0.4.0 confirms sync commits only through followers on 0.4.0 (older
   ones replicate but never report), so until a majority of followers
   run the new version, run the leader with `--sync-commit off` or expect
   "unconfirmed".
3. The leader last: restart it (writes pause while it is down; replicas
   reconnect), or promote a replica, upgrade the old leader and rejoin it.
4. `chron_build_info{version}` on every node shows the new version.
5. 0.5.0 writes records 0.4.0 cannot read as soon as a client sends a
   commit id, and every 0.5.0 client does by default. Never run a 0.4.0
   follower under a 0.5.0 leader (it stops applying with "unknown flag
   bits 0x08" and sync commits go unconfirmed), and there is no
   downgrade of a directory once an id-carrying transaction is in it.
   Clients last: a 0.4.0 daemon drops the id, and a 0.5.0 client's wider
   retry rule would then write twice, so until every daemon runs 0.5.0
   keep clients on `commit_ids: false`.

## Reading the metrics

`chrond --metrics-listen 127.0.0.1:9100` serves `GET /metrics`. Useful
PromQL:

- Commit rate: `rate(chron_commits_total{result="ok"}[5m])`
- Conflict ratio: `rate(chron_commits_total{result="conflict"}[5m]) / rate(chron_commits_total[5m])`
- Commit p99: `histogram_quantile(0.99, sum by (le) (rate(chron_commit_duration_seconds_bucket[5m])))`
- Mean fsync: `rate(chron_fsync_seconds_total[5m]) / rate(chron_fsyncs_total[5m])`
- fsync p99: `histogram_quantile(0.99, sum by (le) (rate(chron_fsync_seconds_bucket[5m])))`
- fsyncs per transaction (segment syncs plus index checkpoints): `(rate(chron_fsyncs_total[5m]) + rate(chron_index_checkpoints_total[5m])) / rate(chron_transactions_committed_total[5m])`; well under 1 under load, 1 + 1/256 at one writer; `--index-checkpoint-batches 1` restores the 0.4 rule of a durable index commit per batch (two fsyncs) where a deployment wants it
- Error ratio by code: `sum by (code) (rate(chron_rpc_calls_total{code!="OK"}[5m]))`
- Group commit effectiveness: `rate(chron_transactions_committed_total[5m]) / rate(chron_commit_batches_total[5m])`
- Gateway error ratio: `sum by (route, status) (rate(chron_http_requests_total{status=~"5.."}[5m]))`
- Leader changes: `increase(chron_leader_changes_total[1h])`
- Restores over the wire: `increase(chron_restores_total[1h])`, and an
  alert on `increase(chron_restores_total{result="error"}[10m]) > 0`
- Sync wait p99: `histogram_quantile(0.99, sum by (le) (rate(chron_sync_commit_wait_seconds_bucket[5m])))`
- Confirmation lag on the leader: `chron_head - chron_majority_applied`
- Lease health on the leader: `min_over_time(chron_lease_remaining_seconds[5m])`

Alerts worth having:

- `chron_replica_connected == 0` for 1 m on a replica.
- `chron_replication_lag_events > N` for 1 m.
- `chron_role{role="fenced"} == 1` (writes are being refused).
- `chron_role{role="candidate"} == 1` for 30 s, or `chron_leader_known == 0`
  on every node (no leader).
- `sum(chron_role{role="leader"}) != 1` across a group for 30 s.
- `increase(chron_sync_commit_timeouts_total[5m]) > 0`.
- A sudden drop in `chron_streams_open{method="/chron.v1.Chron/Subscribe"}`.
