#!/usr/bin/env tarantool
--
-- Vinyl Workload A: Update-Heavy (modeled after YCSB-A / RocksDB overwrite)
--
-- Default mix: 50% point reads, 50% updates on existing keys.
-- Default distribution: Zipfian (hot keys accessed much more often).
-- Purpose: stress overlapping runs, fuse filter effectiveness for
-- point lookups, and the overlap-based compaction driver.
--
-- Dataset: NUM_KEYS rows (default: 2M × scale), VALUE_SIZE bytes per value.
--
-- Usage:
--   tarantool vinyl_workload_a.lua [minutes] [scale] [batch_size]
--
-- The workload is resumable: restarting continues from existing data.
-- WARMUP_SECONDS defaults to 0; measurement duration defaults depend on scale.
--

local log = require('log')
local clock = require('clock')
local fio = require('fio')
local benchmark = dofile(fio.pathjoin(fio.dirname(arg[0]), 'benchmark.lua'))
local config = benchmark.config('a', arg)
benchmark.prepare(config)
local NUM_KEYS = config.num_keys
local BATCH_SIZE = config.batch_size
local LOAD_BATCH = 5000
local NUM_FIBERS = config.workers

box.cfg{
    log       = 'picodata.log',
    log_level = 'info',
    txn_isolation = 'snapshot',
    checkpoint_count    = 2,
    checkpoint_interval = config.checkpoint_interval,
    vinyl_cache         = config.vinyl_cache,
    vinyl_memory        = config.vinyl_memory,
}

benchmark.log_config(config)
benchmark.event('setup', 'start')

-- Schema (idempotent).
if box.space.bench_a == nil then
    local s = box.schema.space.create('bench_a', {
        engine = 'vinyl',
        format = {
            { name = 'id',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', { parts = { 'id' },  })
    log.info('bench_a: created space')
end

local space = box.space.bench_a

-------------------------------------------------------------------------------
-- Key and operation selection
-------------------------------------------------------------------------------

local random_key = benchmark.key_generator(config)
local next_operation = benchmark.operation_selector(config, {'read', 'update'})

-------------------------------------------------------------------------------
-- Random value generator
-------------------------------------------------------------------------------

local random_value = benchmark.value_generator(config.value_size)

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    if count >= NUM_KEYS then
        log.info('bench_a: data already loaded (%d rows)', count)
        return
    end
    log.info('bench_a: loading data from key %d to %d ...', count + 1, NUM_KEYS)
    local batch = 0
    local t0 = clock.monotonic()
    box.begin()
    for i = count + 1, NUM_KEYS do
        space:replace({ i, random_value() })
        batch = batch + 1
        if batch >= LOAD_BATCH then
            box.commit()
            box.begin()
            batch = 0
            if i % math.max(100000, math.floor(NUM_KEYS / 20)) == 0 then
                local elapsed = clock.monotonic() - t0
                local rate = i / elapsed
                local eta = (NUM_KEYS - i) / rate
                log.info('bench_a: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, NUM_KEYS / 1e6, rate, eta)
            end
        end
    end
    if batch > 0 then
        box.commit()
    else
        box.rollback()
    end
    local elapsed = clock.monotonic() - t0
    log.info('bench_a: load complete, %d rows in %.1fs', space:count(), elapsed)
    benchmark.snapshot()
end

load_data()
benchmark.event('setup', 'end')

-------------------------------------------------------------------------------
-- Workload fibers
-------------------------------------------------------------------------------

local function worker(stats)
    benchmark.transaction(stats, 'tx_mixed', function()
        local completed = {reads = 0, writes = 0, read_miss = 0, logical_write_bytes = 0}
        for _ = 1, BATCH_SIZE do
            local key = random_key(NUM_KEYS)
            if next_operation() == 'read' then
                stats.op_attempts = stats.op_attempts + 1
                local tuple = space:get(key)
                completed.reads = completed.reads + 1
                if tuple == nil then completed.read_miss = completed.read_miss + 1 end
            else
                local value = random_value()
                stats.op_attempts = stats.op_attempts + 1
                local tuple = space:replace({key, value})
                completed.logical_write_bytes = completed.logical_write_bytes + tuple:bsize()
                completed.writes = completed.writes + 1
            end
        end
        return completed
    end)
end

math.randomseed(config.seed)
local workers = {}
for i = 1, NUM_FIBERS do workers[i] = worker end
benchmark.run(space, workers, config)
os.exit(0)
