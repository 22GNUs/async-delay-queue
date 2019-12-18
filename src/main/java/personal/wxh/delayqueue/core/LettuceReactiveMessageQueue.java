package personal.wxh.delayqueue.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Getter;
import lombok.NonNull;
import personal.wxh.delayqueue.util.GlobalObjectMapper;
import personal.wxh.delayqueue.util.ReactiveMessageJsonFormatter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 基于lettuce实现的队列, 配合 {@link LettuceReactiveMessageDelayQueue <T>} 使用
 *
 * @author wangxinhua
 * @since 1.0
 */
public class LettuceReactiveMessageQueue<T> implements ReactiveMessageQueue<T> {

  /**
   * 外部传入客户端, 内部进行连接初始化
   *
   * @param key key
   * @param redisClient redis客户端
   * @return 队列实例
   */
  public static LettuceReactiveMessageQueue<Object> connect(
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
  public static <T> LettuceReactiveMessageQueue<T> connect(
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
  public static <T> LettuceReactiveMessageQueue<T> create(
      @NonNull String key,
      @NonNull Class<T> metaClass,
      @NonNull RedisReactiveCommands<String, String> commands) {
    return new LettuceReactiveMessageQueue<>(key, metaClass, commands);
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
   * @apiNote 如果跟 {@link LettuceReactiveMessageDelayQueue <T>} 配合使用建议使用同一个
   */
  @Getter private final ReactiveMessageJsonFormatter<T> formatter;

  public LettuceReactiveMessageQueue(
      String key, Class<T> metaClazz, RedisReactiveCommands<String, String> commands) {
    this.key = key;
    this.metaClazz = metaClazz;
    this.commands = commands;
    this.formatter =
        new ReactiveMessageJsonFormatter<>(GlobalObjectMapper.getInstance(), metaClazz);
  }

  @Override
  public Mono<Long> enqueue(@NonNull Message<T> values) {
    return formatter.writeValue(values).flatMap(json -> commands.rpush(key, json));
  }

  @Override
  public Mono<Long> enqueueBatch(@NonNull Iterable<Message<T>> values) {
    return Flux.fromIterable(values)
        .flatMap(formatter::writeValue)
        .collectList()
        .flatMap(s -> commands.rpush(key, s.toArray(new String[0])));
  }

  @Override
  public Mono<Message<T>> dequeue() {
    return commands.lpop(key).flatMap(formatter::readValue);
  }

  @Override
  public Flux<Message<T>> dequeueBatch(int start, int end) {
    return commands.lrange(key, start, end).flatMap(formatter::readValue);
  }

  @Override
  public Mono<Long> delete() {
    return commands.del(key);
  }

  /**
   * 写入json, 同时转换为scoredValue
   *
   * @param message 消息对象
   * @return scoredValue
   */
  private Mono<ScoredValue<String>> writeAndScored(Message<T> message) {
    return formatter.writeValue(message).map(json -> ScoredValue.just(message.getScore(), json));
  }
}
