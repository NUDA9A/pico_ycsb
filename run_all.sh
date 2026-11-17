#!/bin/bash
#
# Run selected Vinyl workloads and preserve their results.
#
# Usage: run_all.sh <tarantool_binary> <prefix> [scale] [a|b|c|all]
#   e.g.: run_all.sh /path/to/src/tarantool baseline
#          run_all.sh /path/to/src/tarantool baseline_s100 100
#
# Results are stored in /var/opt/bench/<prefix>_{a,b,c}/

set -e

TARANTOOL="$1"
PREFIX="$2"
SCALE="${3:-10}"
WORKLOAD="${4:-all}"
RUN_GROUP="${RUN_ID-$PREFIX}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="${OUTPUT_DIR-/var/opt/bench}"

if [ -z "$TARANTOOL" ] || [ -z "$PREFIX" ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <tarantool_binary> <prefix> [scale] [a|b|c|all]"
    exit 1
fi
for name in "$PREFIX" "$RUN_GROUP"; do
    case "$name" in
        ''|.|..|*[!a-zA-Z0-9_.-]*) echo "Invalid prefix or RUN_ID: $name" >&2; exit 1 ;;
    esac
done
case "$WORKLOAD" in
    all) WORKLOADS="a b c" ;;
    a|b|c) WORKLOADS="$WORKLOAD" ;;
    *) echo "Workload must be a, b, c or all" >&2; exit 1 ;;
esac

if [ ! -x "$TARANTOOL" ]; then
    echo "Error: $TARANTOOL is not executable"
    exit 1
fi
TARANTOOL="$(cd "$(dirname "$TARANTOOL")" && pwd)/$(basename "$TARANTOOL")"
mkdir -p -- "$BENCH_DIR"
BENCH_DIR="$(cd "$BENCH_DIR" && pwd)"
for w in $WORKLOADS; do
    DIR="${BENCH_DIR}/${PREFIX}_${w}"
    if [ -e "$DIR" ] || [ -L "$DIR" ]; then
        echo "Result directory already exists: $DIR" >&2
        exit 1
    fi
done
for w in $WORKLOADS; do mkdir -- "${BENCH_DIR}/${PREFIX}_${w}"; done
export SEED="${SEED-$(($(date +%s) % 2147483647))}"

echo "Binary: $("$TARANTOOL" --version | head -1)"
echo "Prefix: $PREFIX"
echo "Scale: $SCALE"
echo "Environment overrides (WORKERS: A/C; WRITERS/READERS: B):"
for parameter in NUM_KEYS VALUE_SIZE VINYL_MEMORY VINYL_CACHE BATCH_SIZE \
                 DURATION_SECONDS WARMUP_SECONDS WORKERS WRITERS READERS \
                 READ_RATIO UPDATE_RATIO INSERT_RATIO DELETE_RATIO SCAN_RATIO \
                 KEY_DISTRIBUTION ZIPF_THETA HOTSET_SIZE CHECKPOINT_MODE CHECKPOINT_INTERVAL \
                 WRITE_MODE SEED TARANTOOL_GIT_SHA; do
    echo "${parameter}: ${!parameter-<default>}"
done
echo "Resolved parameters are recorded as Configuration in picodata.log."
echo ""

for w in $WORKLOADS; do
    DIR="${BENCH_DIR}/${PREFIX}_${w}"
    SCRIPT="${SCRIPT_DIR}/vinyl_workload_${w}.lua"
    echo "=== Workload ${w^^} ==="
    (cd "$DIR" && RUN_ID="${RUN_GROUP}_${w}" OUTPUT_DIR="$DIR" "$TARANTOOL" "$SCRIPT" "" "$SCALE")
    echo ""
    echo "--- Results ---"
    sed -n '/=== FINAL REPORT ===/,/=== END REPORT ===/p' \
        "$DIR/picodata.log" | sed 's/.*I> //'
    echo ""
done

echo "=== All workloads complete ==="
