#!/usr/bin/env bash
#
# run_benchmarks.sh -- reproduce every number in the README.
#
# Outputs a plain-text table to stdout and to results.txt so the run can be
# attached to a bug report or a pull request that touches the hot path.
#
# Usage: ./scripts/run_benchmarks.sh [count] [size]
#
# Pin the two roles to two cores so the scheduler does not decide the outcome.
# Override PIN to empty on machines where taskset is unavailable.

set -u

COUNT="${1:-50000}"
SIZE="${2:-256}"
BUILD_DIR="${BUILD_DIR:-build}"
BIN="$BUILD_DIR/bench_ipc"
PIN="${PIN:-taskset -c 2,3}"
OUT="results.txt"

if [ ! -x "$BIN" ]; then BIN="$BUILD_DIR/benchmarks/bench_ipc"; fi
if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found. Build first:  cmake -B build && cmake --build build -j"
    exit 1
fi

{
    echo "tachyon benchmark run"
    echo "date      : $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "host      : $(uname -srm)"
    echo "cpu       : $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs)"
    echo "cores     : $(nproc)"
    echo "kernel    : $(uname -r)"
    echo "compiler  : $(${CC:-gcc} --version | head -1)"
    echo "messages  : $COUNT x $SIZE bytes"
    echo "pinning   : ${PIN:-none}"
    echo
    echo "=== LATENCY (round-trip nanoseconds, ${SIZE}B) ==="
} | tee "$OUT"

run_latency() {
    local label="$1" transport="$2" env_val="$3"
    rm -f /dev/shm/tachyon_bench_ipc 2>/dev/null
    local line
    line=$(timeout 300 $PIN env $env_val "$BIN" "$transport" latency "$SIZE" "$COUNT" 2>&1 | head -1)
    printf '%-24s %s\n' "$label" "$line" | tee -a "$OUT"
}

run_latency "tachyon (spin)"   tachyon "TY_NOTIFY=spin"
run_latency "tachyon (hybrid)" tachyon "TY_NOTIFY=hybrid"
run_latency "tachyon (futex)"  tachyon "TY_NOTIFY=futex"
run_latency "pipe"             pipe    "X=1"
run_latency "unix socket"      uds     "X=1"
run_latency "tcp loopback"     tcp     "X=1"

{
    echo
    echo "=== THROUGHPUT (1 KiB messages) ==="
} | tee -a "$OUT"

run_throughput() {
    local label="$1" transport="$2" env_val="$3"
    rm -f /dev/shm/tachyon_bench_ipc 2>/dev/null
    local line
    line=$(timeout 300 $PIN env $env_val "$BIN" "$transport" throughput 1024 200000 2>&1 | head -1)
    printf '%-24s %s\n' "$label" "$line" | tee -a "$OUT"
}

run_throughput "tachyon (spin)"   tachyon "TY_NOTIFY=spin"
run_throughput "tachyon (hybrid)" tachyon "TY_NOTIFY=hybrid"
run_throughput "pipe"             pipe    "X=1"
run_throughput "unix socket"      uds     "X=1"
run_throughput "tcp loopback"     tcp     "X=1"

rm -f /dev/shm/tachyon_bench_ipc 2>/dev/null

echo | tee -a "$OUT"
echo "results written to $OUT" | tee -a "$OUT"
