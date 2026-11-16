#!/usr/bin/env tarantool
--
-- Vinyl Workload B: Time-Series Append
-- (modeled after YCSB-D / RocksDB fillseq + readrandom)
--
-- Write path: sequential inserts at the end of the key space
-- (monotonically increasing keys, like timestamps).
-- Read path: 80% reads favor recent data (latest 1%), 20% uniform.
-- Mix: WRITERS and READERS (4 each by default); ratios depend on throughput.
--
-- Dataset: NUM_KEYS rows initially (default: 2M × scale), then keeps growing.
--
-- Purpose: exercises non-overlapping run detection, range splits
-- without compaction, and the MinHash sketch overlap estimation.
-- With shape-based compaction, many unnecessary compactions happen.
-- With sketch-based compaction, disjoint runs are left alone.
--
-- Usage:
--   tarantool vinyl_workload_b.lua [minutes] [scale]
--
-- Existing data is reused; next_key is derived from the row count, not max key.
-- WARMUP_SECONDS defaults to 0; measurement duration defaults depend on scale.
--

local log = require('log')
local clock = require('clock')
local fio = require('fio')
local benchmark = dofile(fio.pathjoin(fio.dirname(arg[0]), 'benchmark.lua'))
local config = benchmark.config('b', arg)
local INITIAL_KEYS = config.num_keys
local BATCH_SIZE = config.batch_size
local LOAD_BATCH = 5000
local NUM_WRITERS = config.writers
local NUM_READERS = config.readers

box.cfg{
    log       = 'picodata.log',
    log_level = 'info',
    checkpoint_count    = 2,
    checkpoint_interval = 600,
    vinyl_cache         = config.vinyl_cache,
    vinyl_memory        = config.vinyl_memory,
}

benchmark.log_config(config)
benchmark.event('setup', 'start')

-- Schema (idempotent).
if box.space.bench_b == nil then
    local s = box.schema.space.create('bench_b', {
        engine = 'vinyl',
        format = {
            { name = 'ts',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', {
        parts = { 'ts' },
        run_count_per_level = 2,
    })
    log.info('bench_b: created space')
end

local space = box.space.bench_b

-------------------------------------------------------------------------------
-- Random value generator
-------------------------------------------------------------------------------

local random_value = benchmark.value_generator(config.value_size)

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    if count >= INITIAL_KEYS then
        log.info('bench_b: data already loaded (%d rows)', count)
        return count
    end
    log.info('bench_b: loading data from key %d to %d ...', count + 1, INITIAL_KEYS)
    local batch = 0
    local t0 = clock.monotonic()
    box.begin()
    for i = count + 1, INITIAL_KEYS do
        space:replace({ i, random_value() })
        batch = batch + 1
        if batch >= LOAD_BATCH then
            box.commit()
            box.begin()
            batch = 0
            if i % math.max(100000, math.floor(INITIAL_KEYS / 20)) == 0 then
                local elapsed = clock.monotonic() - t0
                local rate = i / elapsed
                local eta = (INITIAL_KEYS - i) / rate
                log.info('bench_b: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, INITIAL_KEYS / 1e6, rate, eta)
            end
        end
    end
    if batch > 0 then
        box.commit()
    else
        box.rollback()
    end
    local elapsed = clock.monotonic() - t0
    log.info('bench_b: load complete, %d rows in %.1fs', space:count(), elapsed)
    benchmark.snapshot()
    return INITIAL_KEYS
end

local loaded = load_data()
benchmark.event('setup', 'end')

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_key = loaded + 1

local function writer(stats)
    local base = next_key
    next_key = next_key + BATCH_SIZE
    benchmark.transaction(stats, function()
        for i = 0, BATCH_SIZE - 1 do
            local value = random_value()
            stats.op_attempts = stats.op_attempts + 1
            space:replace({base + i, value})
        end
        return {inserts = BATCH_SIZE}
    end)
end

local function reader(stats)
    benchmark.operations(stats, function()
        for _ = 1, BATCH_SIZE do
            local max_key = next_key - 1
            if max_key < 1 then return end
            local key
            if math.random() < 0.8 then
                local recent_start = math.max(1, math.floor(max_key * 0.99))
                key = math.random(recent_start, max_key)
            else
                key = math.random(1, max_key)
            end
            stats.op_attempts = stats.op_attempts + 1
            local tuple = space:get(key)
            stats.reads = stats.reads + 1
            if tuple == nil then stats.read_miss = stats.read_miss + 1 end
        end
    end)
end

math.randomseed(tonumber(clock.realtime64() % 2147483647))
local workers = {}
for _ = 1, NUM_WRITERS do workers[#workers + 1] = writer end
for _ = 1, NUM_READERS do workers[#workers + 1] = reader end
benchmark.run(space, workers, config)
os.exit(0)
