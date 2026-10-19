-- enqueue.lua: store a new job and put it in line, atomically.
--
-- KEYS[1]  job hash key    e.g. drover:job:{emails}:7F3A
-- KEYS[2]  ready ZSET key  e.g. drover:{emails}:ready
-- ARGV[1]  id
-- ARGV[2]  queue
-- ARGV[3]  type
-- ARGV[4]  payload
-- ARGV[5]  max_attempts
--
-- Returns the job id.

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

redis.call('HSET', KEYS[1],
    'id', ARGV[1],
    'queue', ARGV[2],
    'type', ARGV[3],
    'payload', ARGV[4],
    'attempts', 0,
    'max_attempts', ARGV[5],
    'state', 'ready',
    'created_at', now_ms
)

redis.call('ZADD', KEYS[2], now_ms, ARGV[1])

return ARGV[1]