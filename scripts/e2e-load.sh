#!/usr/bin/env bash
# A load run against release daemons started here: one node, or three
# peers with sync commits. Prints one summary line (throughput, p50,
# p99, fsyncs per transaction on the leader and on a follower) and can
# keep the report and the scrapes.
#
#   scripts/e2e-load.sh [--no-fsync] [--nodes 1|3] [--writers N]
#       [--in-flight N] [--secs N] [--payload-bytes N]
#       [--index-checkpoint-batches N] [--json-out FILE] [--metrics-out FILE]
set -euo pipefail

fsync=1 nodes=1 writers=4 in_flight=1 secs=15 payload=200 json_out="" metrics_out="" ckpt="" bin="" legacy=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-fsync) fsync=0 ;;
        # Prebuilt binaries (no build); --legacy for 0.4.0 ones, which
        # take no --in-flight (writers are multiplied instead) and count
        # no index checkpoints (every batch paid one: the ratio doubles).
        --bin) bin="$2"; shift ;;
        --legacy) legacy=1 ;;
        --nodes) nodes="$2"; shift ;;
        --writers) writers="$2"; shift ;;
        --in-flight) in_flight="$2"; shift ;;
        --secs) secs="$2"; shift ;;
        --payload-bytes) payload="$2"; shift ;;
        --index-checkpoint-batches) ckpt="$2"; shift ;;
        --json-out) json_out="$2"; shift ;;
        --metrics-out) metrics_out="$2"; shift ;;
        *) echo "unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done
cd "$(git rev-parse --show-toplevel)"
if [[ -z "$bin" ]]; then
    cargo build --release -p chrond -p chron-load >/dev/null
    bin=target/release
fi
work="$(mktemp -d)"
pids=()
cleanup() {
    for p in ${pids[@]+"${pids[@]}"}; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap cleanup EXIT

base_port=$((30000 + RANDOM % 20000))
addrs=()
for ((i = 0; i < nodes; i++)); do
    addrs+=("http://127.0.0.1:$((base_port + i))")
done
for ((i = 0; i < nodes; i++)); do
    args=(--data-dir "$work/node$i" --listen "127.0.0.1:$((base_port + i))"
          --metrics-listen "127.0.0.1:$((base_port + 100 + i))" --log-format json)
    [[ "$fsync" == 0 ]] && args+=(--no-fsync)
    [[ -n "$ckpt" ]] && args+=(--index-checkpoint-batches "$ckpt")
    if [[ "$nodes" -gt 1 ]]; then
        args+=(--lease-ttl 2 --advertise "${addrs[$i]}")
        for ((j = 0; j < nodes; j++)); do
            [[ $j != "$i" ]] && args+=(--peer "${addrs[$j]}")
        done
    fi
    "$bin/chrond" "${args[@]}" >"$work/node$i.log" 2>&1 &
    pids+=($!)
done
list="$(IFS=,; echo "${addrs[*]}")"
# Wait for a leader.
for _ in $(seq 1 100); do
    if "$bin/chron-load" --addr "$list" --duration 0 --writers 0 --readers 0 --subscribers 0 --json >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done

sync_flag=()
[[ "$nodes" -gt 1 ]] && sync_flag=(--sync)
flight_flag=(--in-flight "$in_flight")
if [[ "$legacy" == 1 ]]; then
    flight_flag=()
    writers=$((writers * in_flight))
fi
report="$("$bin/chron-load" --addr "$list" --duration "$secs" --writers "$writers" \
    ${flight_flag[@]+"${flight_flag[@]}"} --aggregates 64 --events-per-txn 2 --payload-bytes "$payload" \
    --readers 1 --subscribers 1 --json ${sync_flag[@]+"${sync_flag[@]}"})"
[[ -n "$json_out" ]] && printf '%s\n' "$report" >"$json_out"

scrape() { curl -s "http://127.0.0.1:$((base_port + 100 + $1))/metrics"; }
ratio() {
    # (fsyncs + checkpoints) / transactions from one scrape; a 0.4.0
    # daemon reports no checkpoints and paid one per batch.
    awk -v legacy="$legacy" '
        /^chron_fsyncs_total\{db="default"\}/ { f = $2 }
        /^chron_index_checkpoints_total\{db="default"\}/ { c = $2 }
        /^chron_transactions_committed_total\{db="default"\}/ { t = $2 }
        END { if (legacy == 1) c = f; if (t > 0) printf "%.3f", (f + c) / t; else print "n/a" }'
}
all_metrics=""
leader_ratio="n/a" follower_ratio="n/a" sync_p99="n/a"
for ((i = 0; i < nodes; i++)); do
    m="$(scrape $i)"
    all_metrics+="# node $i"$'\n'"$m"$'\n'
    if echo "$m" | grep -q '^chron_role{db="default",role="leader"} 1'; then
        leader_ratio="$(echo "$m" | ratio)"
        sync_p99="$(echo "$m" | awk '/^chron_sync_commit_wait_seconds_count/ {c=$2} END {print (c>0)?c:"0"}')"
    elif [[ "$follower_ratio" == "n/a" ]]; then
        follower_ratio="$(echo "$m" | ratio)"
    fi
done
[[ -n "$metrics_out" ]] && printf '%s' "$all_metrics" >"$metrics_out"

python3 - "$report" "$leader_ratio" "$follower_ratio" "$nodes" "$fsync" "$secs" "$writers" "$in_flight" "$legacy" <<'PY'
import json, sys
r = json.loads(sys.argv[1])
secs, writers, in_flight, legacy = float(sys.argv[6]), int(sys.argv[7]), int(sys.argv[8]), sys.argv[9] == "1"
tps = r.get("transactions_per_s", r["transactions"] / secs)
flight = f"{writers} writers (legacy, closed loop)" if legacy else f"{r['writers']} writers x {r['in_flight']} in flight"
print(
    f"nodes {sys.argv[4]} fsync {'on' if sys.argv[5]=='1' else 'off'}  "
    f"{tps:.0f} txn/s  p50 {r['commit_p50_ms']:.2f} ms  p99 {r['commit_p99_ms']:.2f} ms  "
    f"{flight}  "
    f"leader fsyncs/txn {sys.argv[2]}  follower fsyncs/txn {sys.argv[3]}  "
    f"ok {r['ok']} errors {r['errors']} conflicts {r['conflicts']}"
)
PY
