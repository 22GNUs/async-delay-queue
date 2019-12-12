package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
