-- dequeue.lua: take the oldest ready job and lease it to the caller, atomically.
--
-- KEYS[1]  ready ZSET   e.g. drover:{emails}:ready
-- KEYS[2]  leased ZSET  e.g. drover:{emails}:leased
-- ARGV[1]  job key prefix, e.g. drover:job:{emails}:
-- ARGV[2]  lease duration in ms, e.g. 30000
--
-- Returns the job id, or nil if the queue is empty.

local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then
    return false
end

local id = popped[1]
local job_key = ARGV[1] .. id

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

redis.call('HSET', job_key, 'state', 'active')
redis.call('HINCRBY', job_key, 'attempts', 1)

redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[2]), id)

return id
