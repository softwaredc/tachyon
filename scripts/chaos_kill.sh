#!/usr/bin/env bash
#
# chaos_kill.sh -- prove the library survives processes being murdered.
#
# A fast IPC library that loses chunks when a peer is SIGKILLed is worse than a
# slow one, because the failure is invisible: throughput just quietly degrades
# hours later. This script kills producers and consumers at random moments,
# thousands of times, and then asserts that the pool returned to exactly its
# starting free count.
#
# Usage: ./chaos_kill.sh [rounds]
#
# Exit 0 means every chunk ever loaned was accounted for.

set -u

ROUNDS="${1:-2000}"
BUILD_DIR="${BUILD_DIR:-build}"
BIN="$BUILD_DIR/tests/test_chaos"

if [ ! -x "$BIN" ]; then
    echo "error: $BIN not built yet. Run: make -j && this script lives in scripts/"
    exit 1
fi

echo "tachyon chaos test: $ROUNDS rounds of SIGKILL"
echo "-----------------------------------------------"
start=$(date +%s)
"$BIN" "$ROUNDS"
rc=$?
end=$(date +%s)

echo "-----------------------------------------------"
if [ $rc -eq 0 ]; then
    echo "PASS: no chunk was lost in $ROUNDS kill rounds ($((end - start))s)"
else
    echo "FAIL: leaked chunks detected (exit $rc) after $((end - start))s"
fi
exit $rc
