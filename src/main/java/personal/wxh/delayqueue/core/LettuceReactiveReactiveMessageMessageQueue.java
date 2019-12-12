package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import personal.wxh.delayqueue.util.GlobalObjectMapper;
import personal.wxh.delayqueue.util.ReactiveMessageJsonFormatter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 基于list实现的队列, 配合 {@link LettuceReactiveReactiveMessageDelayQueue <T>} 使用
 *
 * @author wangxinhua
 * @since 1.0
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class LettuceReactiveReactiveMessageMessageQueue<T>
    implements ReactiveMessageQueue<T>, ReactiveMessageJsonFormatter<T> {

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param redisClient redis客户端
   * @return 队列实例
   */
  public static LettuceReactiveReactiveMessageMessageQueue<Object> connect(
      @NonNull String key, @NonNull RedisClient redisClient) {
    return create(key, Object.class, redisClient.connect().reactive());
  }

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param redisClient redis客户端
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveMessageMessageQueue<T> connect(
      @NonNull String key, @NonNull Class<T> metaClass, @NonNull RedisClient redisClient) {
    return create(key, metaClass, redisClient.connect().reactive());
  }

  /**
   * 外部传入命令, 不进行连接初始化
   *
   * @param key key
   * @param metaClass 泛型类型
   * @param commands 异步任务命令
   * @return 队列实例
   */
  public static <T> LettuceReactiveReactiveMessageMessageQueue<T> create(
      @NonNull String key,
      @NonNull Class<T> metaClass,
      @NonNull RedisReactiveCommands<String, String> commands) {
    return new LettuceReactiveReactiveMessageMessageQueue<>(
        key, metaClass, commands, GlobalObjectMapper.getInstance());
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
   * @apiNote 如果跟 {@link LettuceReactiveReactiveMessageDelayQueue <T>} 配合使用建议使用同一个
   */
  @Getter private final ObjectMapper objectMapper;

  @Override
  public Mono<Long> enqueue(@NonNull Message<T> values) {
    return writeValue(values).flatMap(json -> commands.rpush(key, json));
  }

  @Override
  public Flux<Long> enqueueBatch(@NonNull Iterable<Message<T>> values) {
    return Flux.fromIterable(values)
        .flatMap(this::writeValue)
        .flatMap(json -> commands.rpush(key, json));
  }

  @Override
  public Mono<Message<T>> dequeue() {
    return commands.lpop(key).flatMap(this::readValueParametric);
  }

  @Override
  public Flux<Message<T>> dequeueBatch(int start, int end) {
    return commands.lrange(key, start, end).flatMap(this::readValueParametric);
  }

  @Override
  public Mono<Long> delete() {
    return commands.del(key);
  }
}
