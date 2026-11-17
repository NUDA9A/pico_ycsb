#!/usr/bin/env tarantool
--
-- Vinyl Workload B: Time-Series Append
-- (modeled after YCSB-D / RocksDB fillseq + readrandom)
--
-- Write path: sequential inserts at the end of the key space
-- (monotonically increasing keys, like timestamps).
-- Default read path: 80% reads favor recent data (latest 1%), 20% uniform.
-- Default mix: WRITERS and READERS (4 each); ratios depend on throughput.
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
-- Existing data is reused; appends start after the largest existing key.
-- WARMUP_SECONDS defaults to 0; measurement duration defaults depend on scale.
--

local log = require('log')
local clock = require('clock')
local fio = require('fio')
local benchmark = dofile(fio.pathjoin(fio.dirname(arg[0]), 'benchmark.lua'))
local config = benchmark.config('b', arg)
benchmark.prepare(config)
local INITIAL_KEYS = config.num_keys
local BATCH_SIZE = config.batch_size
local LOAD_BATCH = 5000
local NUM_WRITERS = config.writers
local NUM_READERS = config.readers
local MAX_KEY = 9007199254740991

box.cfg{
    log       = 'picodata.log',
    log_level = 'info',
    checkpoint_count    = 2,
    checkpoint_interval = config.native_checkpoint_interval,
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
local random_key = benchmark.key_generator(config)

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    local last = space.index.pk:max()
    assert(last == nil or last[1] <= MAX_KEY, 'existing keys must be <= 2^53-1')
    local max_key = last and tonumber(last[1]) or 0
    local missing = math.max(0, INITIAL_KEYS - count)
    assert(missing <= MAX_KEY - max_key, 'initial load would exhaust the exact integer key range')
    if missing == 0 then
        log.info('bench_b: data already loaded (%d rows)', count)
        return max_key
    end
    log.info('bench_b: loading %d rows from key %d to %d ...', missing, max_key + 1, max_key + missing)
    local batch = 0
    local t0 = clock.monotonic()
    box.begin()
    for i = 1, missing do
        space:insert({ max_key + i, random_value() })
        batch = batch + 1
        if batch >= LOAD_BATCH then
            box.commit()
            box.begin()
            batch = 0
            if i % math.max(100000, math.floor(INITIAL_KEYS / 20)) == 0 then
                local elapsed = clock.monotonic() - t0
                local rate = i / elapsed
                local eta = (missing - i) / rate
                log.info('bench_b: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         (count + i) / 1e6, INITIAL_KEYS / 1e6, rate, eta)
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
    return max_key + missing
end

local last_key = load_data()
benchmark.event('setup', 'end')

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_key = last_key + 1
local append = config.write_mode == 'insert' and space.insert or space.replace

local function writer(stats)
    local base = next_key
    assert(BATCH_SIZE <= MAX_KEY - base + 1, 'append would exhaust the exact integer key range')
    next_key = next_key + BATCH_SIZE
    benchmark.transaction(stats, 'tx_append', function()
        local bytes = 0
        for i = 0, BATCH_SIZE - 1 do
            local value = random_value()
            stats.op_attempts = stats.op_attempts + 1
            local tuple = append(space, {base + i, value})
            bytes = bytes + tuple:bsize()
        end
        return {inserts = BATCH_SIZE, logical_write_bytes = bytes}
    end)
end

local function reader(stats)
    benchmark.operations(stats, function()
        for _ = 1, BATCH_SIZE do
            local max_key = next_key - 1
            if max_key < 1 then return end
            local key = random_key(max_key)
            stats.op_attempts = stats.op_attempts + 1
            local started = clock.monotonic()
            local tuple = space:get(key)
            benchmark.observe_latency(stats, 'get', started)
            stats.reads = stats.reads + 1
            if tuple == nil then stats.read_miss = stats.read_miss + 1 end
        end
    end)
end

math.randomseed(config.seed)
local workers = {}
if config.mix_mode == 'dedicated' then
    for _ = 1, NUM_WRITERS do workers[#workers + 1] = writer end
    for _ = 1, NUM_READERS do workers[#workers + 1] = reader end
else
    local next_operation = benchmark.operation_selector(config, {'read', 'insert'})
    local function mixed(stats)
        if next_operation() == 'read' then reader(stats) else writer(stats) end
    end
    for i = 1, config.workers do workers[i] = mixed end
end
benchmark.run(space, workers, config)
os.exit(0)
