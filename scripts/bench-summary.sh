#!/usr/bin/env bash
# A markdown summary of a bench run for the CI step summary:
#   scripts/bench-summary.sh LOAD.json METRICS.txt BENCH.txt
# Exits 1 when the load ran without fsync or the leader paid half an
# fsync or more per transaction (group commit not working).
set -euo pipefail
load="$1" metrics="$2" bench="$3"
python3 - "$load" "$metrics" "$bench" <<'PY'
import json, re, sys
load = json.load(open(sys.argv[1]))
metrics = open(sys.argv[2]).read()
bench = open(sys.argv[3]).read()

def metric(name):
    m = re.search(rf'^{re.escape(name)}\{{db="default"\}} (\S+)$', metrics, re.M)
    return float(m.group(1)) if m else 0.0

f, c, t = metric("chron_fsyncs_total"), metric("chron_index_checkpoints_total"), metric("chron_transactions_committed_total")
ratio = (f + c) / t if t else float("nan")
print("## chron bench\n")
print("| measure | value |\n|---|---|")
print(f"| fsync-on load | {load['transactions_per_s']:.0f} txn/s, p50 {load['commit_p50_ms']:.2f} ms, p99 {load['commit_p99_ms']:.2f} ms, {load['writers']} writers x {load['in_flight']} in flight |")
print(f"| leader fsyncs per transaction | {ratio:.3f} (segment syncs {f:.0f}, index checkpoints {c:.0f}, transactions {t:.0f}) |")
for line in bench.splitlines():
    m = re.match(r"^(append[^ ]*|read[^ ]*)\s+time:\s+\[\S+ \S+ (\S+ \S+) \S+ \S+\]", line.strip())
    if m:
        print(f"| {m.group(1)} | {m.group(2)} |")
    m = re.match(r"^append-concurrent/(\S+): .*fsyncs/txn (\S+)", line.strip())
    if m:
        print(f"| {m.group(1)} fsyncs/txn (library) | {m.group(2)} |")
ok = True
if load.get("fsync") is not True:
    print("\n**the load ran without fsync**"); ok = False
if not (ratio < 0.5):
    print(f"\n**{ratio:.3f} fsyncs per transaction: group commit is not batching**"); ok = False
if not load.get("ok"):
    print(f"\n**the load reported violations**: {load.get('violations')}"); ok = False
sys.exit(0 if ok else 1)
PY
