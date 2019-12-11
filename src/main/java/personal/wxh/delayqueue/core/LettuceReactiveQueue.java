package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import personal.wxh.delayqueue.util.ScriptLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

import static personal.wxh.delayqueue.util.Exceptions.checked;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class LettuceReactiveQueue<T> implements DelayQueue<T> {

  private final String key;

  private final Class<T> clazz;

  /** 执行dequeue的脚本sha1, 初始化时加载 */
  private final String dequeueDigest;

  /** 执行dequeueBatch的脚本sha1, 初始化时加载 */
  private final String dequeueBatchDigest;

  /** reactive 命令操作 */
  private final RedisReactiveCommands<String, String> commands;

  /** json转换 */
  private final ObjectMapper objectMapper;

  @SuppressWarnings("unchecked")
  public LettuceReactiveQueue(String key, RedisClient redisClient) {
    this(key, (Class<T>) Object.class, redisClient);
  }

  public LettuceReactiveQueue(String key, Class<T> clazz, RedisClient redisClient) {
    this(key, clazz, redisClient, new ObjectMapper());
  }

  public LettuceReactiveQueue(
      String key, Class<T> clazz, RedisClient redisClient, ObjectMapper objectMapper) {
    this.key = key;
    // 考虑loadScript公用一个连接
    this.dequeueDigest = ScriptLoader.loadScript(redisClient, "/lua/dequeue.lua");
    this.dequeueBatchDigest = ScriptLoader.loadScript(redisClient, "/lua/dequeue-batch.lua");
    this.commands = redisClient.connect().reactive();
    this.objectMapper = objectMapper;
    // 写入类型信息
    objectMapper.activateDefaultTypingAsProperty(
        objectMapper.getPolymorphicTypeValidator(), ObjectMapper.DefaultTyping.NON_FINAL, "@class");
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    this.clazz = clazz;
  }

  @Override
  public Mono<Long> enqueue(@NonNull Message<T> message) {
    return Mono.fromSupplier(checked(() -> objectMapper.writeValueAsString(message)))
        .flatMap(json -> commands.zadd(key, message.getScore(), json));
  }

  @Override
  public Mono<T> dequeue(long max) {
    return commands
        .<String>evalsha(
            dequeueDigest, ScriptOutputType.VALUE, new String[] {key}, String.valueOf(max))
        .last()
        // 考虑处理json解析异常
        .flatMap(this::readValue);
  }

  @Override
  public Flux<T> dequeueBatch(long max) {
    return dequeueBatch(max, Long.MAX_VALUE);
  }

  @Override
  public Flux<T> dequeueBatch(long max, long limit) {
    return dequeueBatch(0, max, 0, limit);
  }

  @Override
  public Flux<T> dequeueBatch(long min, long max, long offset, long limit) {
    return commands
        .<ArrayList<String>>evalsha(
            dequeueBatchDigest,
            ScriptOutputType.MULTI,
            new String[] {key},
            String.valueOf(min),
            String.valueOf(max),
            String.valueOf(offset),
            String.valueOf(limit))
        .flatMap(Flux::fromIterable)
        .flatMap(this::readValue);
  }

  @Override
  public Mono<Long> delete() {
    return commands.del(key);
  }

  private Mono<T> readValue(String json) {
    val type = objectMapper.getTypeFactory().constructParametricType(Message.class, clazz);
    return Mono.fromSupplier(checked(() -> objectMapper.readValue(json, type)));
  }
}
