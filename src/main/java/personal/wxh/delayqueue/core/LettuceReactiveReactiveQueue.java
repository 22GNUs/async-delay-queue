package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import java.util.List;
import lombok.*;
import personal.wxh.delayqueue.util.GlobalObjectMapper;
import personal.wxh.delayqueue.util.ReactiveJsonFormatter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 基于list实现的队列
 *
 * @author wangxinhua
 * @since 1.0
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class LettuceReactiveReactiveQueue<T> implements ReactiveQueue<T>, ReactiveJsonFormatter<T> {

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param redisClient redis客户端
   * @return 队列实例
   */
  public static LettuceReactiveReactiveQueue<Object> connect(
      @NonNull String key, @NonNull RedisClient redisClient) {
    return connect(key, Object.class, redisClient, null);
  }

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param redisClient redis客户端
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveQueue<T> connect(
      @NonNull String key, @NonNull Class<T> metaClass, @NonNull RedisClient redisClient) {
    return connect(key, metaClass, redisClient, null);
  }

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param redisClient redis客户端
   * @param objectMapper 序列化对象
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveQueue<T> connect(
      @NonNull String key,
      @NonNull Class<T> metaClass,
      @NonNull RedisClient redisClient,
      ObjectMapper objectMapper) {
    val commends = redisClient.connect().reactive();
    return create(key, metaClass, commends, objectMapper);
  }

  /**
   * 外部传入命令, 不进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param commands 异步任务命令
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveQueue<T> create(
      @NonNull String key,
      @NonNull Class<T> metaClass,
      @NonNull RedisReactiveCommands<String, String> commands) {
    return new LettuceReactiveReactiveQueue<>(key, metaClass, commands, null);
  }

  /**
   * 外部传入命令, 不进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param commands 异步任务命令
   * @param objectMapper 序列化对象
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveQueue<T> create(
      @NonNull String key,
      @NonNull Class<T> metaClass,
      @NonNull RedisReactiveCommands<String, String> commands,
      ObjectMapper objectMapper) {
    objectMapper = objectMapper == null ? GlobalObjectMapper.getInstance() : objectMapper;
    return new LettuceReactiveReactiveQueue<>(key, metaClass, commands, objectMapper);
  }

  /** redis队列key */
  private final String key;

  /** 类型class */
  @Getter private final Class<T> metaClazz;

  /** redis异步操作命令对象 */
  private final RedisReactiveCommands<String, String> commands;

  /**
   * 对象序列化, 可以使用外部的objectMapper
   *
   * @apiNote 如果跟 {@link LettuceReactiveReactiveDelayQueue <T>} 配合使用建议使用同一个
   */
  @Getter private final ObjectMapper objectMapper;

  @Override
  public Mono<Long> enqueue(@NonNull T values) {
    return writeValue(values).flatMap(json -> commands.rpush(key, json));
  }

  @Override
  public Flux<Long> enqueueBatch(@NonNull List<T> values) {
    return Flux.fromIterable(values)
        .flatMap(this::writeValue)
        .flatMap(json -> commands.rpush(key, json));
  }

  @Override
  public Mono<T> dequeue() {
    return commands.lpop(key).flatMap(this::readValue);
  }

  @Override
  public Flux<T> dequeueBatch(int start, int end) {
    return commands.lrange(key, start, end).flatMap(this::readValue);
  }
}
