#!/usr/bin/env tarantool
--
-- Vinyl Workload C: Delete Storm + Range Scans
-- (modeled after RocksDB deleterandom + readseq)
--
-- Phase 1 (load): insert N keys uniformly.
-- Default mix: 30% DELETE, 20% REPLACE, 30% SCAN, 20% GET.
-- Each branch performs BATCH_SIZE operations; scans remain independent requests.
--
-- Dataset: NUM_KEYS rows initially (default: 2M × scale), VALUE_SIZE bytes per value.
--
-- Purpose: exercises tombstone accumulation, read amplification
-- detection, and the read-amp compaction driver.  Range scans
-- through tombstone-heavy regions decompress pages only to
-- discard them, which the read-amp tracking should detect and
-- trigger compaction.
--
-- Usage:
--   tarantool vinyl_workload_c.lua [minutes] [scale]
--
-- Resumable: checks existing row count before loading.
-- WARMUP_SECONDS defaults to 0; measurement duration defaults depend on scale.
--

local log = require('log')
local clock = require('clock')
local fio = require('fio')
local benchmark = dofile(fio.pathjoin(fio.dirname(arg[0]), 'benchmark.lua'))
local config = benchmark.config('c', arg)
benchmark.prepare(config)
local NUM_KEYS = config.num_keys
local BATCH_SIZE = config.batch_size
local LOAD_BATCH = 5000
local SCAN_LENGTH = 200
local NUM_FIBERS = config.workers

box.cfg{
    log       = 'picodata.log',
    log_level = 'info',
    checkpoint_count    = 2,
    checkpoint_interval = config.checkpoint_interval,
    vinyl_cache         = config.vinyl_cache,
    vinyl_memory        = config.vinyl_memory,
}

benchmark.log_config(config)
benchmark.event('setup', 'start')

-- Schema (idempotent).
if box.space.bench_c == nil then
    local s = box.schema.space.create('bench_c', {
        engine = 'vinyl',
        format = {
            { name = 'id',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', {
        parts = { 'id' },
    })
    log.info('bench_c: created space')
end

local space = box.space.bench_c

-------------------------------------------------------------------------------
-- Random value generator
-------------------------------------------------------------------------------

local random_value = benchmark.value_generator(config.value_size)
local random_key = benchmark.key_generator(config)
local next_operation = benchmark.operation_selector(config, {'delete', 'insert', 'scan', 'read'})

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    if count >= NUM_KEYS then
        log.info('bench_c: data already loaded (%d rows)', count)
        return
    end
    log.info('bench_c: loading data from key %d to %d ...', count + 1, NUM_KEYS)
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
                log.info('bench_c: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
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
    log.info('bench_c: load complete, %d rows in %.1fs', space:count(), elapsed)
    benchmark.snapshot()
end

load_data()
benchmark.event('setup', 'end')

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_new_key = NUM_KEYS + 1

local function worker(stats)
    local op = next_operation()
    if op == 'delete' then
        benchmark.transaction(stats, 'tx_delete', function()
            local bytes = 0
            for _ = 1, BATCH_SIZE do
                local key = random_key(next_new_key - 1)
                stats.op_attempts = stats.op_attempts + 1
                space:delete(key)
                bytes = bytes + benchmark.delete_bytes(key)
            end
            return {deletes = BATCH_SIZE, logical_write_bytes = bytes}
        end)
    elseif op == 'insert' then
        local base = next_new_key
        next_new_key = next_new_key + BATCH_SIZE
        benchmark.transaction(stats, 'tx_append', function()
            local bytes = 0
            for i = 0, BATCH_SIZE - 1 do
                local value = random_value()
                stats.op_attempts = stats.op_attempts + 1
                local tuple = space:replace({base + i, value})
                bytes = bytes + tuple:bsize()
            end
            return {inserts = BATCH_SIZE, logical_write_bytes = bytes}
        end)
    elseif op == 'scan' then
        benchmark.operations(stats, function()
            for _ = 1, BATCH_SIZE do
                local start_key = random_key(math.max(1, next_new_key - SCAN_LENGTH))
                local count = 0
                stats.op_attempts = stats.op_attempts + 1
                local started = clock.monotonic()
                for _, tuple in space:pairs(start_key, {iterator = 'GE'}) do
                    count = count + 1
                    if count >= SCAN_LENGTH then break end
                end
                benchmark.observe_latency(stats, 'scan', started)
                stats.scans = stats.scans + 1
                stats.scan_rows = stats.scan_rows + count
            end
        end)
    else
        benchmark.operations(stats, function()
            for _ = 1, BATCH_SIZE do
                local key = random_key(next_new_key - 1)
                stats.op_attempts = stats.op_attempts + 1
                local started = clock.monotonic()
                local tuple = space:get(key)
                benchmark.observe_latency(stats, 'get', started)
                stats.reads = stats.reads + 1
                if tuple == nil then stats.read_miss = stats.read_miss + 1 end
            end
        end)
    end
end

math.randomseed(config.seed)
local workers = {}
for i = 1, NUM_FIBERS do workers[i] = worker end
benchmark.run(space, workers, config)
os.exit(0)
