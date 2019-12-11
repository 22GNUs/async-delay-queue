local message = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #message > 0 then
  redis.call('ZREM', KEYS[1], message[1])
  return message
else
  return nil
end
