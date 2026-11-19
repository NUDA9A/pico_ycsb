local clock = require('clock')
local fiber = require('fiber')
local log = require('log')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpack')
local bit = require('bit')
local digest = require('digest')

local M = {}
local REPORT_INTERVAL = 10
local POSTRUN_TIMEOUT = 60
local POSTRUN_QUIET = 1
local RSS_INTERVAL = 1
local LATENCY_KINDS = {'tx_mixed', 'tx_append', 'tx_delete', 'get', 'scan'}
local HIST_FACTOR, HIST_BINS = 1.02, 2048
local HIST_LOG = math.log(HIST_FACTOR)
local output

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
        key_distribution = os.getenv('KEY_DISTRIBUTION') or
                           (workload == 'a' and 'zipfian' or workload == 'b' and 'latest' or 'uniform'),
        zipf_theta = option('ZIPF_THETA', 0.99, 0),
        hotset_size = option('HOTSET_SIZE', 1, 0),
        setup_snapshot = os.getenv('SETUP_SNAPSHOT') or 'on',
        checkpoint_mode = os.getenv('CHECKPOINT_MODE') or 'auto',
        checkpoint_interval = option('CHECKPOINT_INTERVAL', 600, 0),
        seed = option('SEED', tonumber(clock.realtime64() % 2147483647), 0),
        rng_algorithm = 'xoshiro128**-1.1',
        rng_stream_scheme = 'sha256-seed-phase-worker-le32-v1',
        run_id = os.getenv('RUN_ID') or workload .. '-' .. string.format('%.0f', clock.realtime() * 1e6),
        output_dir = os.getenv('OUTPUT_DIR') or '.',
        tarantool_git_sha = os.getenv('TARANTOOL_GIT_SHA'),
        si_window = os.getenv('SI_WINDOW'),
    }
    assert(config.run_id:match('^[%w_.-]+$') and config.run_id ~= '.' and config.run_id ~= '..',
           'RUN_ID must contain only letters, digits, underscores, dots or hyphens')
    assert(config.output_dir ~= '' and not config.output_dir:find('%z'), 'OUTPUT_DIR must be a nonempty path')
    config.output_dir = fio.abspath(config.output_dir)
    assert(config.seed <= 2147483647, 'SEED must be <= 2147483647')
    if config.tarantool_git_sha ~= nil then
        assert(#config.tarantool_git_sha == 40 and config.tarantool_git_sha:match('^%x+$'),
               'TARANTOOL_GIT_SHA must be a full 40-digit hexadecimal commit ID')
    end
    assert(config.si_window == nil or config.si_window == 'auto' or config.si_window == 'zero',
           'SI_WINDOW must be auto or zero')
    assert(config.key_distribution == 'uniform' or config.key_distribution == 'zipfian' or
           (workload == 'b' and config.key_distribution == 'latest'),
           'KEY_DISTRIBUTION must be uniform or zipfian (also latest for B)')
    assert(config.zipf_theta < 1, 'ZIPF_THETA must be in [0, 1)')
    assert(config.hotset_size > 0 and config.hotset_size <= 1, 'HOTSET_SIZE must be in (0, 1]')
    assert(config.setup_snapshot == 'on' or config.setup_snapshot == 'off',
           'SETUP_SNAPSHOT must be on or off')
    assert(config.checkpoint_mode == 'auto' or config.checkpoint_mode == 'off' or
           config.checkpoint_mode == 'forced', 'CHECKPOINT_MODE must be auto, off or forced')
    if config.checkpoint_mode == 'forced' then
        assert(config.checkpoint_interval > 0 and config.checkpoint_interval < duration,
               'forced CHECKPOINT_INTERVAL must be positive and shorter than measurement')
    end
    if workload == 'b' then
        config.write_mode = os.getenv('WRITE_MODE') or 'replace'
        assert(config.write_mode == 'replace' or config.write_mode == 'insert',
               'WRITE_MODE must be replace or insert')
        config.writers = option('WRITERS', 4, 0)
        config.readers = option('READERS', 4, 0)
        config.workers = config.writers + config.readers
        assert(config.workers > 0, 'WRITERS + READERS must be positive')
    else
        config.workers = option('WORKERS', 8, 1)
    end
    for _, name in ipairs({'num_keys', 'value_size', 'vinyl_memory', 'vinyl_cache',
                          'batch_size', 'workers', 'writers', 'readers', 'checkpoint_interval', 'seed'}) do
        local value = config[name]
        if value ~= nil then
            assert(value == math.floor(value) and value <= 9007199254740991,
                   name .. ' must be an exactly representable integer <= 2^53-1')
        end
    end
    if config.checkpoint_mode == 'off' then config.checkpoint_interval = 0 end
    config.native_checkpoint_interval = config.checkpoint_mode == 'auto' and config.checkpoint_interval or 0

    local defaults = {
        a = {{'read', 0.5}, {'update', 0.5}},
        b = {{'read', 0.5}, {'insert', 0.5}},
        c = {{'delete', 0.3}, {'insert', 0.2}, {'scan', 0.3}, {'read', 0.2}},
    }
    config.mix_mode = 'ratio'
    if workload == 'b' and os.getenv('READ_RATIO') == nil and os.getenv('INSERT_RATIO') == nil then
        config.mix_mode = 'dedicated'
    else
        local total = 0
        -- A fixed summation order avoids rounding differences from hash iteration.
        for _, item in ipairs(defaults[workload]) do
            local name = item[1] .. '_ratio'
            config[name] = option(name:upper(), item[2], 0)
            assert(config[name] <= 1, name .. ' must be in [0, 1]')
            total = total + config[name]
        end
        assert(math.abs(total - 1) <= 1e-9, 'operation ratios must sum to 1')
        for _, item in ipairs(defaults[workload]) do
            local name = item[1] .. '_ratio'
            config[name] = config[name] / total
        end
    end
    return config
end

function M.random(seed, phase, worker_id)
    local hash = digest.sha256(string.format('%.0f:%s:%.0f', seed, phase, worker_id))
    local function word(i)
        local a, b, c, d = hash:byte(i, i + 3)
        return bit.bor(a, bit.lshift(b, 8), bit.lshift(c, 16), bit.lshift(d, 24))
    end
    local s0, s1, s2, s3 = word(1), word(5), word(9), word(13)
    if bit.bor(s0, s1, s2, s3) == 0 then s0 = 1 end
    -- Public-domain reference: https://prng.di.unimi.it/xoshiro128starstar.c
    local function next_u32()
        local result = bit.tobit(bit.rol(s1 * 5, 7) * 9)
        local t = bit.lshift(s1, 9)
        s2 = bit.bxor(s2, s0)
        s3 = bit.bxor(s3, s1)
        s1 = bit.bxor(s1, s2)
        s0 = bit.bxor(s0, s3)
        s2 = bit.bxor(s2, t)
        s3 = bit.rol(s3, 11)
        return result < 0 and result + 4294967296 or result
    end
    local function next_u53()
        local high = bit.rshift(next_u32(), 5)
        return high * 67108864 + bit.rshift(next_u32(), 6)
    end
    return function(lower, upper)
        if lower == nil then return next_u53() / 9007199254740992 end
        local range = upper - lower + 1
        local total, draw = 4294967296, next_u32
        if range > total then total, draw = 9007199254740992, next_u53 end
        -- Rejection avoids modulo bias, including for key ranges above 2^32.
        local limit = total - total % range
        local value
        repeat value = draw() until value < limit
        return lower + value % range
    end
end

function M.operation_selector(config, operations, random)
    local choices, total = {}, 0
    for _, operation in ipairs(operations) do
        local ratio = config[operation .. '_ratio']
        if ratio > 0 then
            total = total + ratio
            choices[#choices + 1] = {operation = operation, limit = total}
        end
    end
    return function()
        local u = random() * total
        for _, choice in ipairs(choices) do
            if u < choice.limit then return choice.operation end
        end
        return choices[#choices].operation
    end
end

-- Avoid cancellation in the Zipfian formula when theta is close to one.
local function expm1(x)
    if math.abs(x) < 1e-5 then return x * (1 + x * (0.5 + x * (1 / 6 + x / 24))) end
    return math.exp(x) - 1
end

local function log1p(x)
    if math.abs(x) < 1e-5 then return x * (1 + x * (-0.5 + x * (1 / 3 - x / 4))) end
    return math.log(1 + x)
end

function M.key_generator(config, random)
    local theta, distribution = config.zipf_theta, config.key_distribution
    local alpha = 1 / (1 - theta)
    local zeta_2 = 1 + 0.5 ^ theta
    local previous_n, zeta, eta
    return function(max_key)
        local n = math.max(1, math.floor(max_key * config.hotset_size))
        local offset = config.workload == 'b' and max_key - n or 0
        if distribution == 'latest' then
            if random() < 0.8 then
                return offset + random(math.max(1, math.floor(n * 0.99)), n)
            end
            return offset + random(1, n)
        end
        if distribution == 'uniform' or theta == 0 then return offset + random(1, n) end
        if n == 1 then return offset + 1 end
        local u = random()
        if n == 2 then return offset + (u * zeta_2 < 1 and 1 or 2) end
        if n ~= previous_n then
            -- Euler-Maclaurin approximation, as in the original workload A.
            zeta = expm1((1 - theta) * math.log(n)) / (1 - theta) +
                   0.5 * (1 + n ^ (-theta)) + theta / 12 * (1 - n ^ (-theta - 1))
            eta = -expm1((1 - theta) * math.log(2 / n)) / (1 - zeta_2 / zeta)
            previous_n = n
        end
        local uz = u * zeta
        if uz < 1 then return offset + 1 end
        if uz < zeta_2 then return offset + 2 end
        local key = 1 + math.floor(n * math.exp(log1p(eta * (u - 1)) * alpha))
        return offset + math.min(n, math.max(1, key))
    end
end

function M.value_generator(size, random)
    local bytes, chunks = {}, {}
    return function()
        local count = 0
        -- Bound the argument list to string.char/unpack for large values.
        for offset = 1, size, 256 do
            local length = math.min(256, size - offset + 1)
            for i = 1, length do bytes[i] = random(97, 122) end
            count = count + 1
            chunks[count] = string.char(unpack(bytes, 1, length))
        end
        if count == 1 then return chunks[1] end
        return table.concat(chunks)
    end
end

function M.event(phase, event, details)
    local monotonic, realtime = clock.monotonic(), clock.realtime()
    if output then
        output.events[#output.events + 1] = {phase = phase, event = event,
                                           monotonic = monotonic, realtime = realtime, details = details}
    end
    log.info('phase=%s event=%s monotonic=%.6f realtime=%.6f%s',
             phase, event, monotonic, realtime, details and ' details=' .. json.encode(details) or '')
end

function M.new_stats()
    return {
        reads = 0, writes = 0, inserts = 0, deletes = 0, scans = 0,
        scan_rows = 0, read_miss = 0, op_attempts = 0,
        tx_attempts = 0, tx_committed = 0, tx_failed = 0,
        rollback_calls = 0, rollback_errors = 0, errors = 0,
        logical_write_bytes = 0,
        forced_checkpoints_started = 0, forced_checkpoints_completed = 0,
        forced_checkpoints_failed = 0, forced_checkpoints_skipped = 0,
    }
end

function M.delete_bytes(key)
    return #msgpack.encode({key})
end

function M.observe_latency(stats, kind, started)
    local us = math.max(0, (clock.monotonic() - started) * 1e6)
    local bucket = us <= 1 and 0 or math.ceil(math.log(us) / HIST_LOG)
    bucket = math.min(HIST_BINS + 1, bucket)
    local histogram = stats.latency[kind]
    histogram[bucket] = (histogram[bucket] or 0) + 1
end

local function latency_delta(before, after)
    local result = {}
    for _, kind in ipairs(LATENCY_KINDS) do
        local bins, count = {}, 0
        for bucket, value in pairs(after[kind]) do
            bins[bucket] = value - (before[kind][bucket] or 0)
            count = count + bins[bucket]
        end
        local values = {count = count, overflow = bins[HIST_BINS + 1] or 0}
        for _, p in ipairs({50, 95, 99}) do
            local seen, rank = 0, math.ceil(count * p / 100)
            values['p' .. p .. '_us'] = json.NULL
            if count > 0 then
                for bucket = 0, HIST_BINS do
                    seen = seen + (bins[bucket] or 0)
                    if seen >= rank then
                        values['p' .. p .. '_us'] = HIST_FACTOR ^ bucket
                        break
                    end
                end
            end
        end
        result[kind] = values
    end
    return result
end

local function read_rss()
    local file = io.open('/proc/self/status', 'r')
    if not file then return nil end
    local text = file:read('*a')
    file:close()
    local kb = text and tonumber(text:match('\nVmRSS:%s+(%d+)%s+kB'))
    return kb and kb * 1024 or nil
end

local function new_rss(time, value)
    return {time = time, value = value, area = 0, seconds = 0, peak = value,
            samples = value and 1 or 0, missing = value and 0 or 1}
end

local function update_rss(rss, time, value)
    local dt = time - rss.time
    if rss.value and value then rss.area = rss.area + (rss.value + value) * 0.5 * dt end
    rss.seconds = rss.seconds + dt
    rss.samples = rss.samples + (value and 1 or 0)
    rss.missing = rss.missing + (value and 0 or 1)
    if value then rss.peak = math.max(rss.peak or value, value) end
    rss.time, rss.value = time, value
end

local function rss_summary(rss)
    return {mean_bytes = rss.missing == 0 and rss.seconds > 0 and rss.area / rss.seconds or json.NULL,
            peak_sampled_bytes = rss.peak or json.NULL, samples = rss.samples, missing = rss.missing}
end

local function record_error(stats, err)
    stats.errors = stats.errors + 1
    if stats.errors <= 3 then log.warn('workload error: %s', tostring(err)) end
end

function M.transaction(stats, kind, body)
    stats.tx_attempts = stats.tx_attempts + 1
    local started = clock.monotonic()
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
    M.observe_latency(stats, kind, started)
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
    dump_watermark = 'regulator.dump_watermark', memory_tuple = 'memory.tuple',
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
    local sample = {counters = {}, gauges = {}, latency = {}}
    local vs, idx = box.stat.vinyl(), space.index.pk:stat()
    for key, value in pairs(stats) do
        if type(value) == 'number' then sample.counters[key] = value end
    end
    for kind, histogram in pairs(stats.latency) do
        sample.latency[kind] = {}
        for bucket, count in pairs(histogram) do sample.latency[kind][bucket] = count end
    end
    sample.counters.ops = stats.reads + stats.writes + stats.inserts + stats.deletes + stats.scans
    collect(sample.counters, idx, index_counters)
    collect(sample.counters, vs, vinyl_counters)
    collect(sample.gauges, idx, index_gauges)
    collect(sample.gauges, vs, vinyl_gauges)
    sample.gauges.rss_bytes = read_rss()
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

local function sorted_keys(values)
    local keys = {}
    for key in pairs(values) do keys[#keys + 1] = key end
    table.sort(keys)
    return keys
end

local counter_fields, gauge_fields = M.new_stats(), {rss_bytes = true}
counter_fields.ops = true
for name in pairs(index_counters) do counter_fields[name] = true end
for name in pairs(vinyl_counters) do counter_fields[name] = true end
for name in pairs(index_gauges) do gauge_fields[name] = true end
for name in pairs(vinyl_gauges) do gauge_fields[name] = true end

local function nullable_fields(values, fields)
    local result = {}
    for name in pairs(fields) do result[name] = values[name] or json.NULL end
    return result
end

function M.finish_setup(did_load)
    if did_load and output.config.setup_snapshot == 'on' then
        M.event('setup', 'checkpoint_start')
        local ok, err = pcall(box.snapshot)
        M.event('setup', ok and 'checkpoint_end' or 'checkpoint_failed')
        if not ok then error(err) end
    else
        local reason = did_load and 'disabled' or 'already_loaded'
        M.event('setup', 'checkpoint_skipped', {reason = reason})
    end
    M.event('setup', 'end')
    local fields = {
        memory_level0 = vinyl_gauges.memory_level0,
        dump_watermark = vinyl_gauges.dump_watermark,
        memory_tuple = vinyl_gauges.memory_tuple,
        scheduler_dumps = vinyl_counters.scheduler_dumps,
    }
    local state = {}
    collect(state, box.stat.vinyl(), fields)
    M.event('setup', 'setup_vinyl_state', nullable_fields(state, fields))
end

local columns = {'run_id', 'phase', 'start_monotonic', 'end_monotonic', 'seconds',
                 'OPS', 'TPS_explicit', 'vinyl_written_bytes', 'wa', 'legacy_wa',
                 'read_amplification', 'rss_mean_bytes', 'rss_peak_sampled_bytes',
                 'rss_samples', 'rss_missing'}
for _, name in ipairs(sorted_keys(counter_fields)) do columns[#columns + 1] = name end
for _, name in ipairs(sorted_keys(gauge_fields)) do columns[#columns + 1] = name end
for _, kind in ipairs(LATENCY_KINDS) do
    for _, field in ipairs({'count', 'p50_us', 'p95_us', 'p99_us', 'overflow'}) do
        columns[#columns + 1] = kind .. '_' .. field
    end
end

local function csv_cell(value)
    if value == nil then return '' end
    if type(value) == 'number' then return string.format('%.17g', value) end
    return '"' .. tostring(value):gsub('"', '""') .. '"'
end

local function write_file(path, text)
    local file = assert(fio.open(path, {'O_WRONLY', 'O_CREAT', 'O_EXCL'}, tonumber('644', 8)))
    local ok, err = file:write(text)
    local closed, close_err = file:close()
    assert(ok, err)
    assert(closed, close_err)
end

function M.prepare(config)
    assert(output == nil, 'only one benchmark run per process is supported')
    assert(fio.mktree(config.output_dir))
    local summary_path = fio.pathjoin(config.output_dir, 'summary.json')
    local csv_path = fio.pathjoin(config.output_dir, 'timeseries.csv')
    assert(not fio.lstat(summary_path) and not fio.lstat(csv_path) and not fio.lstat(summary_path .. '.tmp'),
           'result files already exist in OUTPUT_DIR')
    write_file(summary_path, json.encode({schema_version = 1, status = 'incomplete', config = config}) .. '\n')
    local csv = assert(fio.open(csv_path, {'O_WRONLY', 'O_CREAT', 'O_EXCL'}, tonumber('644', 8)))
    local written, err = csv:write(table.concat(columns, ',') .. '\n')
    if not written then csv:close(); error(err) end
    output = {csv = csv, summary_path = summary_path, config = config, events = {}}
end

local function phase_result(phase, before, after, rss, requested)
    local d = M.delta(before.counters, after.counters)
    local dt = after.time - before.time
    local written = add(d.dump_out, d.compaction_out)
    return {phase = phase, start_monotonic = before.time, end_monotonic = after.time,
            seconds = dt, requested_seconds = requested or json.NULL,
            counters = nullable_fields(d, counter_fields),
            gauges_start = nullable_fields(before.gauges, gauge_fields),
            gauges_end = nullable_fields(after.gauges, gauge_fields),
            OPS = M.ratio(d.ops, dt) or json.NULL,
            TPS_explicit = M.ratio(d.tx_committed, dt) or json.NULL,
            vinyl_written_bytes = written or json.NULL,
            wa = M.ratio(written, d.logical_write_bytes) or json.NULL,
            legacy_wa = M.ratio(written, d.dump_in) or json.NULL,
            read_amplification = M.ratio(d.disk_read_bytes, d.get_bytes) or json.NULL,
            latency = latency_delta(before.latency, after.latency), rss = rss_summary(rss)}
end

local function report_interval(phase, before, after, rss)
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
    local data = phase_result(phase, before, after, rss)
    log.info('phase=%s wa_uncompressed=%s legacy_wa=%s rss_mean_bytes=%s rss_peak_sampled_bytes=%s',
             phase, display(data.wa), display(data.legacy_wa),
             display(data.rss.mean_bytes), display(data.rss.peak_sampled_bytes))
    for _, kind in ipairs(LATENCY_KINDS) do
        log.info('phase=%s latency=%s %s', phase, kind, fields_text(data.latency[kind], data.latency[kind]))
    end
    local row = {run_id = output.config.run_id, phase = phase,
                 start_monotonic = before.time, end_monotonic = after.time, seconds = dt,
                 OPS = data.OPS, TPS_explicit = data.TPS_explicit,
                 vinyl_written_bytes = data.vinyl_written_bytes, wa = data.wa,
                 legacy_wa = data.legacy_wa, read_amplification = data.read_amplification}
    for name, value in pairs(d) do row[name] = value end
    for name, value in pairs(after.gauges) do row[name] = value end
    for name, value in pairs(data.rss) do row['rss_' .. name] = value end
    for kind, values in pairs(data.latency) do
        for name, value in pairs(values) do row[kind .. '_' .. name] = value end
    end
    local cells = {}
    for i, name in ipairs(columns) do cells[i] = csv_cell(row[name]) end
    assert(output.csv:write(table.concat(cells, ',') .. '\n'))
end

local function start_checkpoints(stats, started, seconds, on_error)
    local interval = output.config.checkpoint_interval
    local deadline = started + seconds
    local last_tick = math.ceil(seconds / interval) - 1
    local checkpoint = {phase = 'measurement', stop = false, done = false,
                        wake = fiber.channel(1), finished = fiber.channel(1)}
    output.checkpoint = checkpoint
    local driver = fiber.new(function()
        local ok, err = pcall(function()
            local function skip(first, last)
                if last < first then return end
                local count = last - first + 1
                stats.forced_checkpoints_skipped = stats.forced_checkpoints_skipped + count
                M.event(checkpoint.phase, 'checkpoint_skipped', {count = count, first_id = first})
            end
            local tick = 1
            while tick <= last_tick and not checkpoint.stop do
                local scheduled = started + tick * interval
                checkpoint.wake:get(math.max(0, scheduled - clock.monotonic()))
                if checkpoint.stop or clock.monotonic() >= deadline then
                    skip(tick, math.min(last_tick, math.floor((clock.monotonic() - started) / interval)))
                    break
                end
                stats.forced_checkpoints_started = stats.forced_checkpoints_started + 1
                M.event(checkpoint.phase, 'checkpoint_start', {id = tick, scheduled_monotonic = scheduled})
                local saved, save_error = pcall(box.snapshot)
                if saved then
                    stats.forced_checkpoints_completed = stats.forced_checkpoints_completed + 1
                else
                    stats.forced_checkpoints_failed = stats.forced_checkpoints_failed + 1
                end
                M.event(checkpoint.phase, saved and 'checkpoint_end' or 'checkpoint_failed',
                        {id = tick, scheduled_monotonic = scheduled,
                         error = not saved and tostring(save_error) or nil})
                if not saved then error('forced checkpoint failed: ' .. tostring(save_error)) end
                -- Skip elapsed slots instead of issuing a burst of overdue snapshots.
                local elapsed_tick = math.min(last_tick, math.floor((clock.monotonic() - started) / interval))
                skip(tick + 1, elapsed_tick)
                tick = math.max(tick, elapsed_tick) + 1
            end
        end)
        if not ok then
            checkpoint.error = tostring(err)
            on_error(checkpoint.error)
        end
        checkpoint.finished_at = clock.monotonic()
        checkpoint.done = true
        checkpoint.finished:put(true)
    end)
    driver:name('forced_checkpoint')
end

local function stop_checkpoints()
    local checkpoint = output.checkpoint
    if checkpoint and not checkpoint.stop then
        checkpoint.stop = true
        checkpoint.wake:put(true, 0)
    end
    return checkpoint
end

function M.run_phase(space, stats, make_worker, phase, seconds)
    M.number(seconds, phase .. ' seconds', 0)
    local config = output.config
    local workers = {}
    for i = 1, config.workers do
        workers[i] = make_worker(M.random(config.seed, phase, i), i)
    end
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
            if not ok then stop = true; stop_checkpoints() end
            done:put({ok = ok, err = err})
        end)
        fibers[i]:set_joinable(true)
        fibers[i]:name(phase .. '_' .. i)
    end
    M.event(phase, 'start')
    local before = M.sample(space, stats)
    local previous = before
    local rss = new_rss(before.time, before.gauges.rss_bytes)
    local interval_rss = new_rss(before.time, before.gauges.rss_bytes)
    deadline = before.time + seconds
    log.info('phase=%s clock_start=%.6f deadline=%.6f', phase, before.time, deadline)
    -- The main fiber reports, so reporter failure cannot silently lose a fiber.
    local ok, err = pcall(function()
        if phase == 'measurement' and output.config.checkpoint_mode == 'forced' then
            start_checkpoints(stats, before.time, seconds, function(checkpoint_error)
                failure, stop = checkpoint_error, true
            end)
        end
        for _ = 1, #workers do gate:put(true) end
        local remaining = #workers
        local next_report = before.time + REPORT_INTERVAL
        local next_rss = before.time + RSS_INTERVAL
        local stopping = false
        while remaining > 0 do
            local now = clock.monotonic()
            if now >= deadline and not stopping then
                stop, stopping = true, true
                stop_checkpoints()
                M.event(phase, 'stop_requested')
            end
            local wake_at = math.min(next_report, next_rss)
            if not stopping then wake_at = math.min(deadline, wake_at) end
            local result = done:get(math.max(0, wake_at - now))
            if result then
                remaining = remaining - 1
                if not result.ok then failure = tostring(result.err); stop = true end
            end
            if remaining > 0 and clock.monotonic() >= next_report then
                local current = M.sample(space, stats)
                update_rss(rss, current.time, current.gauges.rss_bytes)
                update_rss(interval_rss, current.time, current.gauges.rss_bytes)
                report_interval(phase, previous, current, interval_rss)
                previous = current
                interval_rss = new_rss(current.time, current.gauges.rss_bytes)
                next_report = current.time + REPORT_INTERVAL
                next_rss = current.time + RSS_INTERVAL
            elseif remaining > 0 and clock.monotonic() >= next_rss then
                local value = read_rss()
                local sampled_at = clock.monotonic()
                update_rss(rss, sampled_at, value)
                update_rss(interval_rss, sampled_at, value)
                next_rss = sampled_at + RSS_INTERVAL
            end
        end
    end)
    stop = true
    local checkpoint = stop_checkpoints()
    -- Release any workers still waiting if phase startup failed.
    for _ = 1, #workers do gate:put(true, 0) end
    for _, f in ipairs(fibers) do
        local joined, join_error = f:join()
        if not joined then failure = tostring(join_error) end
    end
    if not ok or failure then
        M.event(phase, 'failed')
        error(failure or err)
    end
    local after = M.sample(space, stats)
    if checkpoint then
        checkpoint.phase = 'postrun'
        checkpoint.wait_deadline = after.time + POSTRUN_TIMEOUT
    end
    M.event(phase, 'end')
    update_rss(rss, after.time, after.gauges.rss_bytes)
    update_rss(interval_rss, after.time, after.gauges.rss_bytes)
    report_interval(phase, previous, after, interval_rss)
    return phase_result(phase, before, after, rss, seconds), after
end

function M.postrun(space, stats, before)
    M.event('postrun', 'start')
    local checkpoint = output.checkpoint
    if checkpoint then checkpoint.wait_deadline = before.time + POSTRUN_TIMEOUT end
    local previous, current = before, before
    local reported = before
    local rss = new_rss(before.time, before.gauges.rss_bytes)
    local interval_rss = new_rss(before.time, before.gauges.rss_bytes)
    local quiet_since = before.time
    local status = 'timeout'
    while current.time - before.time < POSTRUN_TIMEOUT do
        if checkpoint and checkpoint.error then status = 'checkpoint_failed'; break end
        local available = current.gauges.tasks_inprogress ~= nil and
                          current.counters.tasks_completed ~= nil
        if not available and (not checkpoint or checkpoint.done) then
            status = 'unavailable'
            break
        end
        fiber.sleep(0.1)
        current = M.sample(space, stats)
        update_rss(rss, current.time, current.gauges.rss_bytes)
        update_rss(interval_rss, current.time, current.gauges.rss_bytes)
        if checkpoint and checkpoint.error then status = 'checkpoint_failed'; break end
        if (checkpoint and not checkpoint.done) or current.gauges.tasks_inprogress ~= 0 or
           current.counters.tasks_completed ~= previous.counters.tasks_completed or
           current.counters.dump_out ~= previous.counters.dump_out or
           current.counters.compaction_out ~= previous.counters.compaction_out then
            quiet_since = current.time
        end
        if (not checkpoint or checkpoint.done) and current.time - quiet_since >= POSTRUN_QUIET then
            status = 'idle_observed'
            break
        end
        previous = current
        if current.time - reported.time >= REPORT_INTERVAL then
            report_interval('postrun', reported, current, interval_rss)
            reported = current
            interval_rss = new_rss(current.time, current.gauges.rss_bytes)
        end
    end
    current = M.sample(space, stats)
    update_rss(rss, current.time, current.gauges.rss_bytes)
    update_rss(interval_rss, current.time, current.gauges.rss_bytes)
    if checkpoint and checkpoint.error then
        status = 'checkpoint_failed'
    elseif checkpoint and (not checkpoint.done or checkpoint.finished_at > checkpoint.wait_deadline) then
        status = 'checkpoint_timeout'
    end
    report_interval('postrun', reported, current, interval_rss)
    M.event('postrun', status)
    local result = phase_result('postrun', before, current, rss)
    result.status = status
    result.forced_checkpoint_pending = checkpoint ~= nil and not checkpoint.done
    return result
end

function M.summary(result, postrun)
    local d, first, last = result.counters, result.gauges_start, result.gauges_end
    log.info('=== FINAL REPORT ===')
    log.info('Phase:           measurement')
    log.info('Duration:        requested=%.6f actual=%.6f overrun=%.6f',
             result.requested_seconds, result.seconds, math.max(0, result.seconds - result.requested_seconds))
    log.info('Total ops:       %d', d.ops)
    log.info('OPS:             %s', display(result.OPS))
    log.info('TPS (explicit):  %s', display(result.TPS_explicit))
    log.info('Counters:        %s', fields_text(d, M.new_stats()))
    log.info('Errors:          %d', d.errors)
    log.info('Vinyl delta:     %s %s', fields_text(d, index_counters), fields_text(d, vinyl_counters))
    log.info('Vinyl start:     %s %s', fields_text(first, index_gauges), fields_text(first, vinyl_gauges))
    log.info('Vinyl end:       %s %s', fields_text(last, index_gauges), fields_text(last, vinyl_gauges))
    log.info('Dump I/O:        in=%s out=%s', display(d.dump_in), display(d.dump_out))
    log.info('Compaction I/O:  in=%s out=%s', display(d.compaction_in), display(d.compaction_out))
    log.info('Vinyl output (uncompressed): %s', display(result.vinyl_written_bytes))
    log.info('Logical written: %s', display(d.logical_write_bytes))
    log.info('WA (uncompressed, measurement): %s', display(result.wa))
    log.info('legacy_wa:       %s', display(result.legacy_wa))
    log.info('Read amplification: %s', display(result.read_amplification))
    log.info('Space amplification (estimate): %s', display(result.space_amplification_estimate))
    log.info('RSS:             %s', fields_text(result.rss, result.rss))
    for _, kind in ipairs(LATENCY_KINDS) do
        log.info('Latency %s: %s', kind, fields_text(result.latency[kind], result.latency[kind]))
    end
    log.info('Postrun:         %s (excluded from measurement)', postrun.status)
    log.info('=== END REPORT ===')
end

function M.run(space, make_worker, config)
    local seconds, warmup = config.duration_seconds, config.warmup_seconds
    log.info('workload=%s workers=%d warmup_seconds=%.6f measurement_seconds=%.6f',
             space.name, config.workers, warmup, seconds)
    local stats = M.new_stats()
    stats.latency = {}
    for _, kind in ipairs(LATENCY_KINDS) do stats.latency[kind] = {} end
    local warmup_result, result, postrun
    local ok, err = pcall(function()
        if warmup > 0 then
            warmup_result = M.run_phase(space, stats, make_worker, 'warmup', warmup)
        else
            M.event('warmup', 'skipped')
        end
        local after
        result, after = M.run_phase(space, stats, make_worker, 'measurement', seconds)
        postrun = M.postrun(space, stats, after)
        assert(not output.checkpoint or not output.checkpoint.error,
               output.checkpoint and output.checkpoint.error)
        assert(postrun.status ~= 'checkpoint_timeout', 'forced checkpoint did not finish within postrun timeout')
        if config.checkpoint_mode == 'forced' then
            assert(result.counters.forced_checkpoints_started > 0,
                   'no forced checkpoint started before the measurement deadline')
        end
        -- shortcut: tuple overhead remains estimated; replace when live serialized size is available cheaply.
        result.space_amplification_estimate = M.ratio(after.gauges.disk_bytes,
            space:count() * (config.value_size + 10)) or json.NULL
        M.summary(result, postrun)
    end)
    local checkpoint = stop_checkpoints()
    if checkpoint and not checkpoint.done then
        local wait_deadline = checkpoint.wait_deadline or clock.monotonic() + POSTRUN_TIMEOUT
        checkpoint.finished:get(math.max(0, wait_deadline - clock.monotonic()))
        if not checkpoint.done then
            ok, err = false, tostring(err or '') .. ' forced checkpoint did not finish before timeout'
            M.event(checkpoint.phase, 'checkpoint_timeout', {error = err})
        end
    end
    if checkpoint and checkpoint.error then ok, err = false, checkpoint.error end
    local closed, close_err = output.csv:close()
    if not closed then
        ok, err = false, tostring(err or '') .. ' CSV close failed: ' .. tostring(close_err)
    end
    if not ok then M.event('run', 'failed', {error = tostring(err)}) end
    local document = {
        schema_version = 1, status = ok and 'complete' or 'incomplete', config = config,
        error = not ok and tostring(err) or json.NULL,
        forced_checkpoint_pending = checkpoint ~= nil and not checkpoint.done,
        tarantool = {version = box.info.version, git_sha = config.tarantool_git_sha or json.NULL},
        latency_histogram = {unit = 'us', minimum_upper_bound = 1, bucket_factor = HIST_FACTOR,
                             finite_buckets = HIST_BINS + 1, maximum_upper_bound = HIST_FACTOR ^ HIST_BINS},
        rss = {source = '/proc/self/status:VmRSS', interval_seconds = RSS_INTERVAL,
               postrun_interval_seconds = 0.1,
               mean_method = 'trapezoidal', peak_method = 'maximum_observed'},
        vinyl_bytes_basis = 'uncompressed', wa_scope = 'reported_interval_or_phase',
        logical_write_bytes_rule = 'MessagePack tuple for INSERT/REPLACE; MessagePack key array for every successful DELETE; committed transactions only',
        events = output.events, warmup = warmup_result or json.NULL,
        measurement = result or json.NULL, postrun = postrun or json.NULL,
    }
    local temporary = output.summary_path .. '.tmp'
    write_file(temporary, json.encode(document) .. '\n')
    assert(fio.rename(temporary, output.summary_path))
    assert(ok, err)
end

return M
