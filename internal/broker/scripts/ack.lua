-- ack.lua: mark a leased job as completed, atomically.
--
-- KEYS[1]  leased ZSET  e.g. drover:{emails}:leased
-- KEYS[2]  job hash     e.g. drover:job:{emails}:7F3A
-- ARGV[1]  job id
--
-- Returns 1 if the job was acked, 0 if it was not leased
-- (never dequeued, or already acked).

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
    return 0
end

redis.call('HSET', KEYS[2], 'state', 'completed')

return 1
