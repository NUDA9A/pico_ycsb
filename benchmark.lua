local clock = require('clock')
local fiber = require('fiber')
local log = require('log')

local M = {}
local REPORT_INTERVAL = 10
local POSTRUN_TIMEOUT = 60
local POSTRUN_QUIET = 1

function M.number(value, name, minimum)
    local n = tonumber(value)
    assert(n and n == n and n < math.huge and n >= minimum,
           name .. ' must be a finite number >= ' .. minimum)
    return n
end

local function option(name, default, minimum, argument)
    if argument == nil or argument == '' then argument = os.getenv(name) end
    if argument == nil then argument = default end
    return M.number(argument, name, minimum)
end

function M.config(workload, args)
    args = args or {}
    assert(workload == 'a' or workload == 'b' or workload == 'c', 'unknown workload')
    local scale_arg = args[2]
    if scale_arg == nil or scale_arg == '' then scale_arg = 10 end
    local scale = M.number(scale_arg, 'scale', 0)
    assert(scale > 0, 'scale must be positive')
    local default_minutes = math.max(1, math.floor(scale /
        math.max(1, 2 * math.log10(scale)) + 0.5))
    local default_batch = workload == 'b' and 200 or 50
    if workload == 'a' then
        default_batch = math.max(2, math.floor(scale * math.log10(scale) + 0.5))
    end
    local duration
    if args[1] ~= nil and args[1] ~= '' then
        duration = M.number(args[1], 'minutes', 0) * 60
    else
        duration = option('DURATION_SECONDS', default_minutes * 60, 0)
    end
    M.number(duration, 'duration_seconds', 0)
    assert(duration > 0, 'duration_seconds must be positive')
    local config = {
        workload = workload, scale = scale,
        num_keys = option('NUM_KEYS', 2000000 * scale, 1),
        value_size = option('VALUE_SIZE', 90, 1),
        vinyl_memory = option('VINYL_MEMORY', 4 * 1024 * 1024 * scale, 1),
        vinyl_cache = option('VINYL_CACHE', 0, 0),
        batch_size = option('BATCH_SIZE', default_batch, 1,
                            workload == 'a' and args[3] or nil),
        duration_seconds = duration,
        warmup_seconds = option('WARMUP_SECONDS', 0, 0),
    }
    if workload == 'b' then
        config.writers = option('WRITERS', 4, 0)
        config.readers = option('READERS', 4, 0)
        config.workers = config.writers + config.readers
        assert(config.workers > 0, 'WRITERS + READERS must be positive')
    else
        config.workers = option('WORKERS', 8, 1)
    end
    for _, name in ipairs({'num_keys', 'value_size', 'vinyl_memory', 'vinyl_cache',
                          'batch_size', 'workers', 'writers', 'readers'}) do
        local value = config[name]
        if value ~= nil then
            assert(value == math.floor(value) and value <= 9007199254740991,
                   name .. ' must be an exactly representable integer <= 2^53-1')
        end
    end
    return config
end

function M.value_generator(size)
    local bytes, chunks = {}, {}
    return function()
        local count = 0
        -- Bound the argument list to string.char/unpack for large values.
        for offset = 1, size, 256 do
            local length = math.min(256, size - offset + 1)
            for i = 1, length do bytes[i] = math.random(97, 122) end
            count = count + 1
            chunks[count] = string.char(unpack(bytes, 1, length))
        end
        if count == 1 then return chunks[1] end
        return table.concat(chunks)
    end
end

function M.event(phase, event)
    log.info('phase=%s event=%s monotonic=%.6f realtime=%.6f',
             phase, event, clock.monotonic(), clock.realtime())
end

function M.snapshot()
    M.event('setup', 'checkpoint_start')
    local ok, err = pcall(box.snapshot)
    M.event('setup', ok and 'checkpoint_end' or 'checkpoint_failed')
    if not ok then error(err) end
end

function M.new_stats()
    return {
        reads = 0, writes = 0, inserts = 0, deletes = 0, scans = 0,
        scan_rows = 0, read_miss = 0, op_attempts = 0,
        tx_attempts = 0, tx_committed = 0, tx_failed = 0,
        rollback_calls = 0, rollback_errors = 0, errors = 0,
    }
end

local function record_error(stats, err)
    stats.errors = stats.errors + 1
    if stats.errors <= 3 then log.warn('workload error: %s', tostring(err)) end
end

function M.transaction(stats, body)
    stats.tx_attempts = stats.tx_attempts + 1
    local ok, completed = pcall(function()
        box.begin()
        local result = body()
        box.commit()
        return result
    end)
    if not ok then
        stats.tx_failed = stats.tx_failed + 1
        record_error(stats, completed)
        -- A failed commit may already have rolled back the transaction.
        stats.rollback_calls = stats.rollback_calls + 1
        local rolled_back, err = pcall(box.rollback)
        if not rolled_back then
            stats.rollback_errors = stats.rollback_errors + 1
            error('rollback failed: ' .. tostring(err))
        end
        return false
    end
    stats.tx_committed = stats.tx_committed + 1
    for key, value in pairs(completed) do stats[key] = stats[key] + value end
    return true
end

function M.operations(stats, body)
    local ok, err = pcall(body)
    if not ok then record_error(stats, err) end
    return ok
end

local index_counters = {
    dump_count = 'disk.dump.count', dump_in = 'disk.dump.input.bytes',
    dump_out = 'disk.dump.output.bytes',
    compaction_count = 'disk.compaction.count',
    compaction_in = 'disk.compaction.input.bytes',
    compaction_out = 'disk.compaction.output.bytes',
    bloom_hit = 'disk.iterator.bloom.hit', bloom_miss = 'disk.iterator.bloom.miss',
    get_bytes = 'get.bytes', disk_read_bytes = 'disk.iterator.read.bytes',
}
local vinyl_counters = {
    scheduler_dumps = 'scheduler.dump_count',
    tasks_completed = 'scheduler.tasks_completed', tasks_failed = 'scheduler.tasks_failed',
    cache_hit = 'index_cache.hit', cache_miss = 'index_cache.miss',
    cache_evict = 'index_cache.evict',
    dict_attempted = 'dict.train_attempted', dict_throttled = 'dict.train_throttled',
    dict_failed = 'dict.train_failed', dict_rejected = 'dict.train_rejected',
    dict_accepted = 'dict.train_accepted',
}
local index_gauges = {
    ranges = 'range_count', runs = 'run_count', run_avg = 'run_avg', disk_bytes = 'disk.bytes',
}
local vinyl_gauges = {
    tasks_inprogress = 'scheduler.tasks_inprogress', memory_level0 = 'memory.level0',
    memory_tx = 'memory.tx', cache_memory = 'index_cache.mem_used',
    dict_active = 'dict.dicts_active', dict_bytes = 'dict.dicts_bytes',
}

local function collect(target, source, fields)
    for name, path in pairs(fields) do
        local value = source
        for part in path:gmatch('[^.]+') do
            if type(value) ~= 'table' then value = nil; break end
            value = value[part]
        end
        target[name] = tonumber(value)
    end
end

function M.sample(space, stats)
    local sample = {counters = {}, gauges = {}}
    local vs, idx = box.stat.vinyl(), space.index.pk:stat()
    for key, value in pairs(stats) do sample.counters[key] = value end
    sample.counters.ops = stats.reads + stats.writes + stats.inserts + stats.deletes + stats.scans
    collect(sample.counters, idx, index_counters)
    collect(sample.counters, vs, vinyl_counters)
    collect(sample.gauges, idx, index_gauges)
    collect(sample.gauges, vs, vinyl_gauges)
    sample.time = clock.monotonic()
    return sample
end

function M.delta(before, after)
    local result = {}
    for key, value in pairs(after) do
        if before[key] ~= nil then
            assert(value >= before[key], 'counter decreased: ' .. key)
            result[key] = value - before[key]
        end
    end
    return result
end

function M.ratio(numerator, denominator)
    if numerator == nil or denominator == nil or denominator <= 0 then return nil end
    return numerator / denominator
end

local function add(a, b)
    if a ~= nil and b ~= nil then return a + b end
end

local function display(value)
    return value == nil and 'N/A' or tostring(value)
end

local function fields_text(values, fields)
    local names, text = {}, {}
    for name in pairs(fields) do names[#names + 1] = name end
    table.sort(names)
    for _, name in ipairs(names) do text[#text + 1] = name .. '=' .. display(values[name]) end
    return table.concat(text, ' ')
end

function M.log_config(config)
    log.info('Configuration: %s', fields_text(config, config))
end

local function report_interval(phase, before, after)
    local dt = after.time - before.time
    if dt <= 0 then return end
    local d = M.delta(before.counters, after.counters)
    log.info('phase=%s interval_start=%.6f interval_end=%.6f seconds=%.6f '
             .. 'ops=%d OPS=%.3f TPS_explicit=%.3f %s',
             phase, before.time, after.time, dt, d.ops, d.ops / dt,
             d.tx_committed / dt, fields_text(d, M.new_stats()))
    log.info('phase=%s vinyl_delta %s %s', phase,
             fields_text(d, index_counters), fields_text(d, vinyl_counters))
    log.info('phase=%s vinyl_current %s %s', phase,
             fields_text(after.gauges, index_gauges), fields_text(after.gauges, vinyl_gauges))
end

function M.run_phase(space, stats, workers, phase, seconds)
    M.number(seconds, phase .. ' seconds', 0)
    assert(#workers > 0, 'at least one worker is required')
    local deadline
    local stop, failure = false, nil
    local gate, done = fiber.channel(#workers), fiber.channel(#workers)
    local fibers = {}
    for i, step in ipairs(workers) do
        fibers[i] = fiber.new(function()
            gate:get()
            local ok, err = pcall(function()
                while not stop and clock.monotonic() < deadline do
                    step(stats)
                    fiber.sleep(0)
                end
            end)
            if not ok then stop = true end
            done:put({ok = ok, err = err})
        end)
        fibers[i]:set_joinable(true)
        fibers[i]:name(phase .. '_' .. i)
    end
    M.event(phase, 'start')
    local before = M.sample(space, stats)
    local previous = before
    deadline = before.time + seconds
    log.info('phase=%s clock_start=%.6f deadline=%.6f', phase, before.time, deadline)
    for _ = 1, #workers do gate:put(true) end

    -- The main fiber reports, so reporter failure cannot silently lose a fiber.
    local ok, err = pcall(function()
        local remaining = #workers
        local next_report = before.time + REPORT_INTERVAL
        local stopping = false
        while remaining > 0 do
            local now = clock.monotonic()
            if now >= deadline and not stopping then
                stop, stopping = true, true
                M.event(phase, 'stop_requested')
            end
            local wake_at = stopping and next_report or math.min(deadline, next_report)
            local result = done:get(math.max(0, wake_at - now))
            if result then
                remaining = remaining - 1
                if not result.ok then failure = tostring(result.err); stop = true end
            end
            if remaining > 0 and clock.monotonic() >= next_report then
                local current = M.sample(space, stats)
                report_interval(phase, previous, current)
                previous = current
                next_report = current.time + REPORT_INTERVAL
            end
        end
    end)
    stop = true
    for _, f in ipairs(fibers) do
        local joined, join_error = f:join()
        if not joined then failure = tostring(join_error) end
    end
    if not ok or failure then
        M.event(phase, 'failed')
        error(failure or err)
    end
    local after = M.sample(space, stats)
    report_interval(phase, previous, after)
    M.event(phase, 'end')
    return {before = before, after = after, requested = seconds,
            elapsed = after.time - before.time,
            counters = M.delta(before.counters, after.counters)}
end

function M.postrun(space, stats, before)
    M.event('postrun', 'start')
    local previous, current = before, before
    local quiet_since = before.time
    local status = 'timeout'
    while current.time - before.time < POSTRUN_TIMEOUT do
        if current.gauges.tasks_inprogress == nil or
           current.counters.tasks_completed == nil then
            status = 'unavailable'
            break
        end
        fiber.sleep(0.1)
        current = M.sample(space, stats)
        if current.gauges.tasks_inprogress ~= 0 or
           current.counters.tasks_completed ~= previous.counters.tasks_completed or
           current.counters.dump_out ~= previous.counters.dump_out or
           current.counters.compaction_out ~= previous.counters.compaction_out then
            quiet_since = current.time
        end
        if current.time - quiet_since >= POSTRUN_QUIET then
            status = 'idle_observed'
            break
        end
        previous = current
    end
    report_interval('postrun', before, current)
    M.event('postrun', status)
    return status
end

function M.summary(space, result, postrun_status, value_size)
    local d, first, last = result.counters, result.before.gauges, result.after.gauges
    local written = add(d.dump_out, d.compaction_out)
    log.info('=== FINAL REPORT ===')
    log.info('Phase:           measurement')
    log.info('Duration:        requested=%.6f actual=%.6f overrun=%.6f',
             result.requested, result.elapsed, math.max(0, result.elapsed - result.requested))
    log.info('Total ops:       %d', d.ops)
    log.info('OPS:             %s', display(M.ratio(d.ops, result.elapsed)))
    log.info('TPS (explicit):  %s', display(M.ratio(d.tx_committed, result.elapsed)))
    log.info('Counters:        %s', fields_text(d, M.new_stats()))
    log.info('Errors:          %d', d.errors)
    log.info('Vinyl delta:     %s %s', fields_text(d, index_counters), fields_text(d, vinyl_counters))
    log.info('Vinyl start:     %s %s', fields_text(first, index_gauges), fields_text(first, vinyl_gauges))
    log.info('Vinyl end:       %s %s', fields_text(last, index_gauges), fields_text(last, vinyl_gauges))
    log.info('Dump I/O:        in=%s out=%s', display(d.dump_in), display(d.dump_out))
    log.info('Compaction I/O:  in=%s out=%s', display(d.compaction_in), display(d.compaction_out))
    log.info('Total written:   %s', display(written))
    log.info('legacy_wa:       %s', display(M.ratio(written, d.dump_in)))
    log.info('Read amplification: %s', display(M.ratio(d.disk_read_bytes, d.get_bytes)))
    -- shortcut: 10 bytes of key/tuple overhead is an estimate; replace when serialized sizes are measured.
    log.info('Space amplification (estimate): %s',
             display(M.ratio(last.disk_bytes, space:count() * (value_size + 10))))
    log.info('Postrun:         %s (excluded from measurement)', postrun_status)
    log.info('=== END REPORT ===')
end

function M.run(space, workers, config)
    local seconds, warmup = config.duration_seconds, config.warmup_seconds
    log.info('workload=%s workers=%d warmup_seconds=%.6f measurement_seconds=%.6f',
             space.name, #workers, warmup, seconds)
    local stats = M.new_stats()
    if warmup > 0 then
        M.run_phase(space, stats, workers, 'warmup', warmup)
    else
        M.event('warmup', 'skipped')
    end
    local result = M.run_phase(space, stats, workers, 'measurement', seconds)
    local postrun_status = M.postrun(space, stats, result.after)
    M.summary(space, result, postrun_status, config.value_size)
end

return M
