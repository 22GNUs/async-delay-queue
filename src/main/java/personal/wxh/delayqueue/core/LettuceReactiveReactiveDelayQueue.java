package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import java.util.ArrayList;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import personal.wxh.delayqueue.util.GlobalObjectMapper;
import personal.wxh.delayqueue.util.ReactiveJsonFormatter;
import personal.wxh.delayqueue.util.ScriptLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * TODO 提供一个queue没有写入另一个队列的副作用 通过lua脚本实现的延迟队列
 *
 * <p>队列dequeue时会向 {@code jobQueueKey} 通过RPUSH写入数据</>
 *
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class LettuceReactiveReactiveDelayQueue<T>
    implements ReactiveDelayQueue<T>, ReactiveJsonFormatter<T> {

  @Getter private final String key;

  @Getter private final String jobQueueKey;

  @Getter private final Class<T> metaClazz;

  @Getter private final ObjectMapper objectMapper;

  /** 执行dequeue的脚本sha1, 初始化时加载 */
  private final String dequeueDigest;

  /** 执行dequeueBatch的脚本sha1, 初始化时加载 */
  private final String dequeueBatchDigest;

  /** reactive 命令操作 */
  private final RedisReactiveCommands<String, String> commands;

  private static final String DEQUEUE_SCRIPT_FILE = "/lua/dequeue-trans.lua";
  private static final String DEQUEUE_BATCH_SCRIPT_FILE = "/lua/dequeue-trans-batch.lua";

  /**
   * 外部传入客户端, 内部进行连接初始化, 默认使用object类型不使用泛型
   *
   * @param key key
   * @param jobQueueKey 任务队列key
   * @param redisClient redis客户端
   */
  public static LettuceReactiveReactiveDelayQueue<Object> connect(
      @NonNull String key, String jobQueueKey, @NonNull RedisClient redisClient) {
    return connect(key, jobQueueKey, Object.class, redisClient);
  }

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param jobQueueKey 任务队列key
   * @param clazz 泛型类型
   * @param redisClient redis客户端
   */
  public static <T> LettuceReactiveReactiveDelayQueue<T> connect(
      @NonNull String key,
      String jobQueueKey,
      @NonNull Class<T> clazz,
      @NonNull RedisClient redisClient) {
    val dequeueDigest = ScriptLoader.loadScript(redisClient, DEQUEUE_SCRIPT_FILE);
    val dequeueBatchDigest = ScriptLoader.loadScript(redisClient, DEQUEUE_BATCH_SCRIPT_FILE);
    val commands = redisClient.connect().reactive();
    return new LettuceReactiveReactiveDelayQueue<>(
        key, jobQueueKey, clazz, commands, dequeueDigest, dequeueBatchDigest);
  }

  /**
   * 外部传入命令及脚本文件, 不进行连接初始化
   *
   * @param key key
   * @param jobQueueKey 任务队列key
   * @param clazz 泛型类型
   * @param commands 异步任务命令
   * @param dequeueDigest 单个出队脚本
   * @param dequeueBatchDigest 批量出队脚本
   */
  public static <T> LettuceReactiveReactiveDelayQueue<T> create(
      @NonNull String key,
      String jobQueueKey,
      @NonNull Class<T> clazz,
      @NonNull RedisReactiveCommands<String, String> commands,
      @NonNull String dequeueDigest,
      @NonNull String dequeueBatchDigest) {
    return new LettuceReactiveReactiveDelayQueue<>(
        key, jobQueueKey, clazz, commands, dequeueDigest, dequeueBatchDigest);
  }

  private LettuceReactiveReactiveDelayQueue(
      String key,
      String jobQueueKey,
      Class<T> metaClazz,
      RedisReactiveCommands<String, String> commands,
      String dequeueDigest,
      String dequeueBatchDigest) {
    this.key = key;
    this.jobQueueKey = jobQueueKey;
    // 考虑loadScript公用一个连接
    this.metaClazz = metaClazz;
    this.commands = commands;
    this.dequeueDigest = dequeueDigest;
    this.dequeueBatchDigest = dequeueBatchDigest;
    this.objectMapper = GlobalObjectMapper.getInstance();
  }

  @Override
  public Mono<Long> enqueue(@NonNull Message<T> message) {
    return writeValue(message).flatMap(json -> commands.zadd(key, message.getScore(), json));
  }

  @Override
  public Mono<T> dequeue(long max) {
    return commands
        .<String>evalsha(
            dequeueDigest,
            ScriptOutputType.VALUE,
            new String[] {key},
            String.valueOf(max),
            jobQueueKey)
        .last()
        // 考虑处理json解析异常
        .flatMap(readValueParametric(Message.class));
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
            jobQueueKey)
        .flatMap(Flux::fromIterable)
        .flatMap(readValueParametric(Message.class));
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
    return commands.del(jobQueueKey);
  }

  /**
   * 清空所有队列数据
   *
   * @return Mono<Long>
   */
  public Mono<Boolean> clearAll() {
    return delete().flatMap(d1 -> deleteJobList().map(d2 -> d1 > 0 && d2 > 0));
  }

  public boolean blockClearAll() {
    return clearAll().blockOptional().orElse(false);
  }
}
