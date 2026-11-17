# pico_ycsb

YCSB-style benchmark suite for [Tarantool](https://www.tarantool.io/) Vinyl,
the LSM-tree storage engine.

Used to produce the benchmark results in
[Как мы пересобрали сборку мусора в Vinyl](https://kostja.github.io/tarantool/vinyl/2026/03/11/vinyl-compaction-scheduler.html).

## Workloads

| Workload | Default pattern | Purpose |
|----------|-----------------|---------|
| **A** | 50% GET / 50% REPLACE, Zipfian | Hot-key contention and overlapping runs |
| **B** | 4 append writers + 4 readers, latest-biased reads | Growing dataset and non-overlapping runs |
| **C** | 30% DELETE / 20% REPLACE / 30% SCAN / 20% GET, uniform | Tombstones and range scans |

A uses mixed transactions. B/C batch writes in transactions; GETs and scans
remain independent requests. B defaults to REPLACE; `WRITE_MODE=insert` selects
true INSERT. C still uses REPLACE for appends.
Each C batch contains `BATCH_SIZE` operations; each scan returns up to 200 rows.

## Quick start

```bash
./run_all.sh /path/to/tarantool my_test
```

Each `/var/opt/bench/my_test_{a,b,c}/` contains `picodata.log`, `summary.json`
and `timeseries.csv`. Existing result directories are rejected, never deleted.

## Usage

```text
./run_all.sh <tarantool_binary> <prefix> [scale] [a|b|c|all]
```

`scale` defaults to 10 and sets defaults for dataset size, memory and runtime.
The workload selector defaults to `all`. `OUTPUT_DIR` overrides the parent
directory; `RUN_ID` overrides the metadata prefix (default: `prefix`).

| Scale | Keys | Approximate data size | Measurement duration |
|-------|------|-----------------------|----------------------|
| 1     | 2M   | 200 MB                | 1 minute             |
| 10    | 20M  | 2 GB                  | 5 minutes            |
| 100   | 200M | 20 GB                 | 25 minutes           |

## Running individual workloads

```bash
tarantool vinyl_workload_a.lua [minutes] [scale] [batch_size]
tarantool vinyl_workload_b.lua [minutes] [scale]
tarantool vinyl_workload_c.lua [minutes] [scale]
```

Keep `benchmark.lua` beside the scripts and use a fresh working directory.
Positional arguments override environment settings; `""` skips an argument.
For direct Lua launches, `OUTPUT_DIR` selects the JSON/CSV directory (default:
current directory); database files and `picodata.log` stay in the working directory.

`./run_si_bench.sh <tarantool_binary> [scale] [run_id]` runs both SI variants
under `OUTPUT_DIR/run_id/{auto,zero}_{a,b,c}/`. The ID defaults to `RUN_ID` or is generated;
an existing run directory is rejected. The `zero` variant requires a build
exporting `vinyl_set_conflict_window`.

## Configuration

Environment variables override defaults independently of `scale` and are
validated before `box.cfg()`. Resolved values are logged as `Configuration:`.

| Variable | Default / meaning |
|----------|-------------------|
| `NUM_KEYS` | Initial keys: `2000000 * scale` |
| `VALUE_SIZE` | Value bytes: `90` |
| `VINYL_MEMORY` | Bytes: `4 MiB * scale` |
| `VINYL_CACHE` | Bytes: `0` |
| `BATCH_SIZE` | A: `max(2, floor(scale * log10(scale) + 0.5))`; B: `200`; C: `50` |
| `WORKERS` | A/C workers: `8` |
| `WRITERS`, `READERS` | B workers: `4` each; either may be zero, both cannot |
| `WRITE_MODE` | B only: `replace` (default) or `insert` |
| `DURATION_SECONDS` | Measurement duration from the scale table |
| `WARMUP_SECONDS` | `0` |
| `KEY_DISTRIBUTION` | `uniform` / `zipfian` / `latest` (B-only); defaults: A `zipfian`, B `latest`, C `uniform` |
| `ZIPF_THETA` | `0.99`, in `[0, 1)` |
| `HOTSET_SIZE` | Eligible fraction of the key range, in `(0, 1]`: `1` |
| `CHECKPOINT_MODE` | `auto` (default), `off` or `forced` |
| `CHECKPOINT_INTERVAL` | Seconds: `600`; `off` forces `0`; `forced` requires `0 < interval < duration` |
| `SEED` | Integer in `[0, 2147483647]`; generated when omitted and saved in the results |
| `RUN_ID` | Run label; generated for direct Lua launches |
| `OUTPUT_DIR` | Result directory for Lua; parent directory for launch scripts |
| `TARANTOOL_GIT_SHA` | Optional full 40-digit SHA of the Tarantool build; unknown if omitted |

Sizes use bytes without suffixes. Counts and byte sizes must be integers.

Hotsets select the first fraction of keys in A/C and the most recent fraction
in B (at least one key). They affect point operations and scan starts, excluding
loading/appends. `latest` reads target the last 1% of that range 80% of the time.

`auto` uses the native checkpoint daemon. `off` disables it. `forced` disables
the daemon and calls `box.snapshot()` at interval multiples from measurement
start, strictly before its deadline. Workers and Vinyl background work continue
normally; elapsed slots are skipped when a snapshot or scheduling takes too long.
The effective daemon interval is logged as `native_checkpoint_interval`.
All modes retain the snapshot after initial loading; forced calls do not run in warmup.

### Operation ratios

Set fractions in `[0, 1]`, summing to 1 for the selected workload:

| Workload | Supported variables and defaults |
|----------|----------------------------------|
| A | `READ_RATIO=0.5 UPDATE_RATIO=0.5` |
| B | `READ_RATIO=0.5 INSERT_RATIO=0.5` (opt-in) |
| C | `DELETE_RATIO=0.3 INSERT_RATIO=0.2 SCAN_RATIO=0.3 READ_RATIO=0.2` |

A selects operations; B/C select equal-sized batches. Ratios describe the
requested mix; successful proportions may differ. B defaults to separate
writers/readers with no guaranteed 50/50 throughput. Setting either B ratio
switches to `WRITERS + READERS` mixed workers. Only the selected workload's
ratio variables apply; set every ratio needed to make their sum 1.

Launch scripts inherit these variables; use individual scripts for different
A/B/C settings. Each launcher shares one seed across its runs. Setup and workload
each start from that seed; scheduling and time limits can still change the sequence
of operations completed by concurrent workers.

## Research scenarios

Examples use separate result directories; sizes and durations require calibration.

```bash
# T1: repeated overwrites of a small working set.
NUM_KEYS=100000 VALUE_SIZE=90 VINYL_MEMORY=67108864 READ_RATIO=0 UPDATE_RATIO=1 \
    KEY_DISTRIBUTION=uniform CHECKPOINT_MODE=off SEED=1 \
    ./run_all.sh /path/to/tarantool t1 10 a

# T3: sequential INSERT-only, using explicit ratios to exclude reads.
WRITE_MODE=insert READ_RATIO=0 INSERT_RATIO=1 WRITERS=4 READERS=0 SEED=1 \
    ./run_all.sh /path/to/tarantool t3 10 b

# T6: T1 with checkpoints under load, every 30 seconds.
NUM_KEYS=100000 VALUE_SIZE=90 VINYL_MEMORY=67108864 READ_RATIO=0 UPDATE_RATIO=1 \
    KEY_DISTRIBUTION=uniform CHECKPOINT_MODE=forced CHECKPOINT_INTERVAL=30 SEED=1 \
    ./run_all.sh /path/to/tarantool t6 10 a
```

T1's RAM residency must be checked using actual memory metrics, including retained
versions and engine overhead. For T2, increase the dataset and reduce `HOTSET_SIZE`.

## Metrics

Every 10 seconds and for the final partial interval, the log reports:

- **OPS:** successfully completed operations per second.
- **TPS (explicit):** successful explicit commits per second.
- Attempts, failures, errors and rollback calls.
- **Latency p50/p95/p99:** successful `tx_mixed` (A), `tx_append` (B/C),
  `tx_delete` (C), and independent `get`/`scan` requests (B/C).
- **RSS:** time-weighted mean and maximum observed process memory on Linux.
- Forced checkpoint starts, completions, failures and skipped slots.
- Vinyl dump/compaction I/O, bloom/cache statistics and memory gauges.

Phases: **setup → warmup → measurement → postrun**. Final counters exclude
setup/warmup; rates use actual elapsed time, including batch completion.
Postrun observes background work separately for up to 60 seconds without
forcing a final flush. An active forced checkpoint is awaited during postrun;
a forced run with no checkpoint starts, a checkpoint failure or a timeout is
incomplete. A scan counts as one operation.

**WA before compression** is `(Δdump_out + Δcompaction_out) / logical_write_bytes`.
`dump_out`, `compaction_out` and `vinyl_written_bytes` use uncompressed output
bytes from Vinyl counters, excluding WAL and device-level writes. Logical bytes
include committed INSERT/REPLACE tuples in MessagePack and the MessagePack
key array for every committed DELETE, including missing keys. Rolled-back writes
are excluded. `legacy_wa` uses `Δdump_in` as its denominator. No writes means WA
is unavailable. This is WA for the reported interval or phase: delayed warmup
dumps may contribute, while data still in RAM has not yet contributed output bytes.

Transaction latency covers the batch body and commit; GET/SCAN latency covers one
request. Histograms use 1 µs as the lowest upper bound and 2% bucket steps;
percentiles are bucket upper bounds, computed separately per interval and phase.
RSS uses approximate `/proc/self/status` values about once per second (0.1 s in
postrun); its sampled peak can miss short spikes. Read and estimated space
amplification also use uncompressed Vinyl counters (`VALUE_SIZE + 10` bytes per live row).

## Result files

- **summary.json:** schema version, resolved configuration, Tarantool version/SHA,
  phase/checkpoint events, warmup, measurement and postrun results. An interrupted or failed
  run retains `status: incomplete`; `complete` means reporting finished, so also
  inspect operation error counters.
- **timeseries.csv:** intervals by phase, OPS/TPS, operation and Vinyl counter
  deltas, end-of-interval gauges, latency in µs, and RSS in bytes.

Unavailable values are `null` in JSON, empty in CSV and `N/A` in logs. Export
files are reserved before setup; existing files are rejected and write failures
abort the run. Handled run failures save available results and the error; abrupt
termination may leave only the initial incomplete summary. No per-operation samples are retained.

## Requirements and status

- A compatible Tarantool build with Vinyl support and enough disk space.
- Reused directories keep existing tuples. B appends after the maximum key and
  preserves gaps; C key allocation on restart still needs work. Use fresh data for comparisons.

Remaining: runtime/reproducibility validation and agreement on the baseline procedure.

## License

BSD-2-Clause
