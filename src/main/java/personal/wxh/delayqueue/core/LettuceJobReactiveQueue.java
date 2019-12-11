package personal.wxh.delayqueue.core;

import static personal.wxh.delayqueue.util.Exceptions.checked;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import java.util.ArrayList;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import personal.wxh.delayqueue.util.ScriptLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * TODO 提供一个queue没有写入另一个队列的副作用
 *
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class LettuceJobReactiveQueue<T> implements DelayQueue<T> {

  private final String key;
  private final String jobListKey;

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
  public LettuceJobReactiveQueue(String key, String jobListKey, RedisClient redisClient) {
    this(key, jobListKey, (Class<T>) Object.class, redisClient);
  }

  public LettuceJobReactiveQueue(
      String key, String jobListKey, Class<T> clazz, RedisClient redisClient) {
    this(key, jobListKey, clazz, redisClient, new ObjectMapper());
  }

  public LettuceJobReactiveQueue(
      @NonNull String key,
      String jobListKey,
      @NonNull Class<T> clazz,
      @NonNull RedisClient redisClient,
      @NonNull ObjectMapper objectMapper) {
    this.key = key;
    this.jobListKey = jobListKey;
    // 考虑loadScript公用一个连接
    this.dequeueDigest = ScriptLoader.loadScript(redisClient, "/lua/dequeue-trans.lua");
    this.dequeueBatchDigest = ScriptLoader.loadScript(redisClient, "/lua/dequeue-trans-batch.lua");
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
            dequeueDigest,
            ScriptOutputType.VALUE,
            new String[] {key},
            String.valueOf(max),
            jobListKey)
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
            String.valueOf(limit),
            jobListKey)
        .flatMap(Flux::fromIterable)
        .flatMap(this::readValue);
  }

  @Override
  public Mono<Long> delete() {
    return commands.del(key);
  }

  /**
   * 清空任务队列
   *
   * @return Mono<Long>
   */
  public Mono<Long> deleteJobList() {
    return commands.del(jobListKey);
  }

  /**
   * 清空所有队列数据
   *
   * @return Mono<Long>
   */
  public Mono<Long> clearAll() {
    return delete().flatMap(d1 -> deleteJobList().map(d2 -> d1 == 1L && d2 == 1L ? 1L : 0L));
  }

  private Mono<T> readValue(String json) {
    val type = objectMapper.getTypeFactory().constructParametricType(Message.class, clazz);
    return Mono.fromSupplier(checked(() -> objectMapper.readValue(json, type)));
  }
}