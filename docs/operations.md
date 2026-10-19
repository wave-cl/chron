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
   retry on their own for `failover_timeout` (30 s) under the rule the
   README states (a commit that may have landed is resent only with an
   exact or `new` expectation); others repoint.
2. Writes the dead leader acknowledged after the last transaction the
   new leader had pulled are lost: the replication lag at that moment
   (`chron_replication_lag_events`; `chron-load --kill-leader-every`
   measures it on your traffic). Tell the owners of that window.
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

A node that stays refused as diverged (its `last_error` says "restore
this node from a backup of the leader": the shared history lies below a
retention floor):

1. `chron --addr $X info` shows `role replica`, `last_error` with
   "diverged" and the leader's address.
2. Stop it, move its data directory aside, restore every database from a
   backup of the leader (next runbook) and remove any `promoted`,
   `fenced` or `vote` marker from the directory.
3. Start it with the group's flags; it follows the leader and catches up.

A leader whose lease keeps expiring (`chron_lease_remaining_seconds` at
0, `chron_heartbeat_failures_total` rising): the majority cannot be
reached within a third of the lease. Check the network between the
nodes before raising `--lease-ttl`; a longer lease is a longer outage
when a leader is cut off, and more writes on it that will be lost.

## Restoring a replica from a backup

1. On the leader (started with `--backup-dir`):
   `chron --addr $LEADER backup full.chronbak` (per database: `--db NAME`).
2. Copy `<backup_dir>/<db>/full.chronbak` to the replica host.
3. `chron restore full.chronbak DIR/<db>` for every database, then
   `chron verify DIR/<db>`. The directory `DIR` is the data directory;
   each database is a subdirectory, `default` at least.
4. Remove any `promoted` or `fenced` marker in `DIR`.
5. `chrond --data-dir DIR --replicate-from $LEADER --replicate-token-file FILE`.
6. Watch `chron_replication_lag_events` reach 0, or
   `chron --addr $REPLICA status --max-lag 0`.

## Retention

1. Take a full backup first (above).
2. Every replica must be caught up (`status --max-lag 0` on each): a
   replica below the leader's floor cannot resume and must be restored.
3. `chron --addr $LEADER retain FLOOR`, then the same on each replica.
4. `chron info` (or `chron_floor`) confirms the floor, which lands on a
   segment boundary at or below FLOOR.

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

## Upgrading

1. Read the CHANGELOG for wire or disk changes.
2. Replicas first: restart each, then `status --max-lag 0`.
3. The leader: restart it (writes pause while it is down; replicas
   reconnect), or promote a replica, upgrade the old leader and rejoin it.
4. `chron_build_info{version}` on every node shows the new version.

## Reading the metrics

`chrond --metrics-listen 127.0.0.1:9100` serves `GET /metrics`. Useful
PromQL:

- Commit rate: `rate(chron_commits_total{result="ok"}[5m])`
- Conflict ratio: `rate(chron_commits_total{result="conflict"}[5m]) / rate(chron_commits_total[5m])`
- Commit p99: `histogram_quantile(0.99, sum by (le) (rate(chron_commit_duration_seconds_bucket[5m])))`
- Mean fsync: `rate(chron_fsync_seconds_total[5m]) / rate(chron_fsyncs_total[5m])`
- Error ratio by code: `sum by (code) (rate(chron_rpc_calls_total{code!="OK"}[5m]))`
- Group commit effectiveness: `rate(chron_transactions_committed_total[5m]) / rate(chron_commit_batches_total[5m])`
- Gateway error ratio: `sum by (route, status) (rate(chron_http_requests_total{status=~"5.."}[5m]))`
- Leader changes: `increase(chron_leader_changes_total[1h])`
- Lease health on the leader: `min_over_time(chron_lease_remaining_seconds[5m])`

Alerts worth having:

- `chron_replica_connected == 0` for 1 m on a replica.
- `chron_replication_lag_events > N` for 1 m.
- `chron_role{role="fenced"} == 1` (writes are being refused).
- `chron_role{role="candidate"} == 1` for 30 s, or `chron_leader_known == 0`
  on every node (no leader).
- `sum(chron_role{role="leader"}) != 1` across a group for 30 s.
- A sudden drop in `chron_streams_open{method="/chron.v1.Chron/Subscribe"}`.
