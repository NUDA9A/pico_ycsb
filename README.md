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
remain independent requests. Inserts in B/C currently use `space:replace()`.
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
| `DURATION_SECONDS` | Measurement duration from the scale table |
| `WARMUP_SECONDS` | `0` |
| `KEY_DISTRIBUTION` | `uniform` / `zipfian` / `latest` (B-only); defaults: A `zipfian`, B `latest`, C `uniform` |
| `ZIPF_THETA` | `0.99`, in `[0, 1)` |
| `HOTSET_SIZE` | Eligible fraction of the key range, in `(0, 1]`: `1` |
| `CHECKPOINT_MODE` | `auto` or `off`; default `auto` |
| `CHECKPOINT_INTERVAL` | Native daemon interval in seconds: `600`; `off` forces `0` |
| `SEED` | Integer in `[0, 2147483647]`; generated when omitted and saved in the results |
| `RUN_ID` | Run label; generated for direct Lua launches |
| `OUTPUT_DIR` | Result directory for Lua; parent directory for launch scripts |
| `TARANTOOL_GIT_SHA` | Optional full 40-digit SHA of the Tarantool build; unknown if omitted |

Sizes use bytes without suffixes. Counts and byte sizes must be integers.

Hotsets select the first fraction of keys in A/C and the most recent fraction
in B (at least one key). They affect point operations and scan starts, excluding
loading/appends. `latest` reads target the last 1% of that range 80% of the time.

Checkpoint settings control the native daemon; Vinyl schedules background work
normally. `off` retains the snapshot after initial loading.

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

```bash
# Overwrites of a small hotset, independent dataset and memory sizes.
NUM_KEYS=100000 VALUE_SIZE=90 VINYL_MEMORY=67108864 \
    READ_RATIO=0 UPDATE_RATIO=1 HOTSET_SIZE=0.01 KEY_DISTRIBUTION=uniform \
    DURATION_SECONDS=60 WARMUP_SECONDS=10 tarantool vinyl_workload_a.lua
```

Launch scripts inherit these variables; use individual scripts for different
A/B/C settings. Each launcher shares one seed across its runs. Setup and workload
each start from that seed; scheduling and time limits can still change the sequence
of operations completed by concurrent workers.

## Metrics

Every 10 seconds and for the final partial interval, the log reports:

- **OPS:** successfully completed operations per second.
- **TPS (explicit):** successful explicit commits per second.
- Attempts, failures, errors and rollback calls.
- **Latency p50/p95/p99:** successful `tx_mixed` (A), `tx_append` (B/C),
  `tx_delete` (C), and independent `get`/`scan` requests (B/C).
- **RSS:** time-weighted mean and maximum observed process memory on Linux.
- Vinyl dump/compaction I/O, bloom/cache statistics and memory gauges.

Phases: **setup → warmup → measurement → postrun**. Final counters exclude
setup/warmup; rates use actual elapsed time, including batch completion.
Postrun observes background work separately for up to 60 seconds without
forcing a flush. A scan counts as one operation regardless of returned rows.

**WA** is `(Δdump_out + Δcompaction_out) / logical_write_bytes`.
Logical bytes include committed REPLACE tuples in MessagePack and the MessagePack
key array for every committed DELETE, including missing keys. Rolled-back writes
are excluded. `legacy_wa` uses `Δdump_in` as its denominator. No writes means WA
is unavailable. Checkpoints and delayed warmup dumps can contribute measurement I/O.

Transaction latency covers the batch body and commit; GET/SCAN latency covers one
request. Histograms use 1 µs as the lowest upper bound and 2% bucket steps;
percentiles are bucket upper bounds, computed separately per interval and phase.
RSS uses approximate `/proc/self/status` values about once per second (0.1 s in
postrun); its sampled peak can miss short spikes. Read amplification and estimated space amplification
(`VALUE_SIZE + 10` bytes per row) are also retained.

## Result files

- **summary.json:** schema version, resolved configuration, Tarantool version/SHA,
  phase events, warmup, measurement and postrun results. An interrupted or failed
  run retains `status: incomplete`; `complete` means reporting finished, so also
  inspect operation error counters.
- **timeseries.csv:** intervals by phase, OPS/TPS, operation and Vinyl counter
  deltas, end-of-interval gauges, latency in µs, and RSS in bytes.

Unavailable values are `null` in JSON, empty in CSV and `N/A` in logs. Export
files are reserved before setup; existing files are rejected and write failures
abort the run. No per-operation samples are retained.

## Requirements and status

- A compatible Tarantool build with Vinyl support and enough disk space.
- Reused directories keep existing tuples; B/C key allocation on restart
  still needs work.

Planned: runtime/reproducibility validation, true INSERT-only and forced-checkpoint scenarios.

## License

BSD-2-Clause
