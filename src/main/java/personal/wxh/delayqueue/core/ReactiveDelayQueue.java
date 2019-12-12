package personal.wxh.delayqueue.core;

import lombok.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author wangxinhua
 * @since 1.0
 */
public interface ReactiveDelayQueue<T> {

  /**
   * 把一条消息放入延时队列
   *
   * @param message 消息对象
   * @return true or false
   */
  Mono<Long> enqueue(@NonNull Message<T> message);

  /**
   * 异步出队列
   *
   * @param max 最大值
   * @return Mono<T> or Mono.empty()
   */
  Mono<Message<T>> dequeue(long max);

  /**
   * 异步批量出队列
   *
   * @param max 最大值
   * @return Flux<T> or Flux.empty()
   */
  Flux<Message<T>> dequeueBatch(long max);

  /**
   * 异步批量出队列
   *
   * @param max 最大值
   * @param limit 限制值
   * @return Flux<T> or Flux.empty()
   */
  Flux<Message<T>> dequeueBatch(long max, long limit);

  /**
   * 异步批量出队列
   *
   * @param min 最小值
   * @param max 最大值
   * @param offset 范围开始
   * @param limit 范围结束
   * @return Flux<T> or Flux.empty()
   */
  Flux<Message<T>> dequeueBatch(long min, long max, long offset, long limit);

  /**
   * 删除key
   *
   * @return 删除结果
   */
  Mono<Long> delete();
}
