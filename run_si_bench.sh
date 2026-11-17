#!/bin/bash
#
# Benchmark snapshot isolation with auto and zero conflict windows.
#
# Usage: run_si_bench.sh <tarantool_binary> [scale] [run_id]
#
# Runs YCSB A, B, C with conflict_window=-1 (auto) and 0 (all slow path).

set -e

TARANTOOL="$1"
SCALE="${2:-10}"
RUN_GROUP="${3:-${RUN_ID-si_$(date -u +%Y%m%dT%H%M%SZ)_$$}}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="${OUTPUT_DIR-/var/opt/bench}"

if [ -z "$TARANTOOL" ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 <tarantool_binary> [scale] [run_id]"
    exit 1
fi
case "$RUN_GROUP" in
    ''|.|..|*[!a-zA-Z0-9_.-]*) echo "Invalid run ID: $RUN_GROUP" >&2; exit 1 ;;
esac
if [ ! -x "$TARANTOOL" ]; then
    echo "Error: $TARANTOOL is not executable" >&2
    exit 1
fi
TARANTOOL="$(cd "$(dirname "$TARANTOOL")" && pwd)/$(basename "$TARANTOOL")"
mkdir -p -- "$BENCH_DIR"
BENCH_DIR="$(cd "$BENCH_DIR" && pwd)"
RUN_DIR="${BENCH_DIR}/${RUN_GROUP}"
mkdir -- "$RUN_DIR"
export SEED="${SEED-$(($(date +%s) % 2147483647))}"

echo "Binary: $("$TARANTOOL" --version | head -1)"
echo "Scale: $SCALE"
echo "Results: $RUN_DIR"
echo "Environment overrides (WORKERS: A/C; WRITERS/READERS: B):"
for parameter in NUM_KEYS VALUE_SIZE VINYL_MEMORY VINYL_CACHE BATCH_SIZE \
                 DURATION_SECONDS WARMUP_SECONDS WORKERS WRITERS READERS \
                 READ_RATIO UPDATE_RATIO INSERT_RATIO DELETE_RATIO SCAN_RATIO \
                 KEY_DISTRIBUTION ZIPF_THETA HOTSET_SIZE CHECKPOINT_MODE CHECKPOINT_INTERVAL \
                 SEED TARANTOOL_GIT_SHA; do
    echo "${parameter}: ${!parameter-<default>}"
done
echo "Resolved parameters are recorded as Configuration in picodata.log."
echo ""

# Create a Lua snippet that adds SI metrics to any workload.
cat > "${RUN_DIR}/si_metrics_patch.lua" << 'PATCH'
-- SI metrics reporter (injected by run_si_bench.sh).
local _si_reporter_started = false
if not _si_reporter_started then
    _si_reporter_started = true
    require('fiber').create(function()
        local log = require('log')
        local fiber = require('fiber')
        while true do
            fiber.sleep(10)
            local vs = box.stat.vinyl()
            local tx = vs.tx or {}
            log.info('bench: SI window=%d miss=%d conflict=%d',
                     tx.conflict_window or 0,
                     tx.conflict_window_miss or 0,
                     tx.conflict or 0)
            log.info('bench: memory tx=%d level0=%d',
                     vs.memory.tx or 0, vs.memory.level0 or 0)
        end
    end)
end
PATCH

for window in auto zero; do
    for w in a b c; do
        PREFIX="${window}_${w}"
        DIR="${RUN_DIR}/${PREFIX}"
        SCRIPT="${SCRIPT_DIR}/vinyl_workload_${w}.lua"

        echo "=== Workload ${w^^}, window=${window} ==="
        mkdir -- "$DIR"

        # Build patched script: original + SI metrics + window setting.
        PATCHED="${DIR}/workload.lua"
        cp "$SCRIPT" "$PATCHED"
        cp "${SCRIPT_DIR}/benchmark.lua" "${DIR}/benchmark.lua"

        # All workloads have the same setup-end marker, including B.
        if [ "$window" = "zero" ]; then
            sed -i '/^benchmark[.]event.*setup.*end/a \
local _ffi = require("ffi")\
_ffi.cdef("void vinyl_set_conflict_window(int64_t w)")\
_ffi.C.vinyl_set_conflict_window(0)\
require("log").info("bench: conflict_window forced to 0")\
dofile("../si_metrics_patch.lua")' "$PATCHED"
        else
            sed -i '/^benchmark[.]event.*setup.*end/a \
require("log").info("bench: conflict_window = auto")\
dofile("../si_metrics_patch.lua")' "$PATCHED"
        fi

        (cd "$DIR" && RUN_ID="${RUN_GROUP}_${PREFIX}" OUTPUT_DIR="$DIR" SI_WINDOW="$window" \
            "$TARANTOOL" "$PATCHED" "" "$SCALE")
        echo ""
        echo "--- Results ---"
        grep -E '(OPS|TPS|Errors|bench: SI|bench: memory)' \
            "$DIR/picodata.log" | sed 's/.*I> //' | tail -30
        echo ""
    done
done

echo "=== All SI benchmark runs complete ==="
