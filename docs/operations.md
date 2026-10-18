# Operating chron

Runbooks for a leader with replicas. `$LEADER` and `$REPLICA` are
`http://host:4242`-style addresses; the admin token travels in
`CHRON_ADMIN_TOKEN` (or `--admin-token`). `chron status` exits 0 when the
node is healthy, 1 when it is degraded (a replica disconnected, a fenced
node, lag above `--max-lag`), 2 when unreachable.

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

Alerts worth having:

- `chron_replica_connected == 0` for 1 m on a replica.
- `chron_replication_lag_events > N` for 1 m.
- `chron_role{role="fenced"} == 1` (writes are being refused).
- A sudden drop in `chron_streams_open{method="/chron.v1.Chron/Subscribe"}`.
