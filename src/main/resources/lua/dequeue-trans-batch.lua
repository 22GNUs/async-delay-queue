-- 出队列的同时把数据push到job队列

local zset_key = KEYS[1]

-- 最小值范围, 可选
local min_score = ARGV[1]
local max_score = ARGV[2]

-- offset与limit 可选
local offset = ARGV[3]
local limit = ARGV[4]

local job_list_key = ARGV[5]
-- TYPE命令的返回结果是{'ok':'zset'}这样子,这里利用next做一轮迭代
local status, type = next(redis.call('TYPE', zset_key))
if status ~= nil and status == 'ok' then
    if type == 'zset' then
        local list = redis.call('ZRANGEBYSCORE', zset_key, min_score, max_score, 'LIMIT', offset, limit)
        if list ~= nil and #list > 0 then
            redis.call('ZREM', zset_key, unpack(list))
            -- push
            redis.call('RPUSH', job_list_key, unpack(list))
            return list
        end
    end
end
return nil
