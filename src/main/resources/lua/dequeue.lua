local zset_key = KEYS[1]
local max_score = ARGV[1]

local message = redis.call('ZRANGEBYSCORE', zset_key, '-inf', max_score, 'LIMIT', 0, 1)
if #message > 0 then
    local m = message[1]
    redis.call('ZREM', zset_key, m)
    return message
else
    return nil
end
