package personal.wxh.delayqueue.core;

import java.util.List;
import lombok.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author wangxinhua
 * @since 1.0
 */
public interface ReactiveMessageQueue<T> {

  /**
   * 入队
   *
   * @param value 入队值
   * @return 成功数量, 数量应该等于1
   */
  Mono<Long> enqueue(@NonNull T value);

  /**
   * 批量入队
   *
   * @param values 入队值
   * @return 成功数量, 数量应该等于values长度
   */
  Flux<Long> enqueueBatch(@NonNull List<T> values);

  /**
   * 出队
   *
   * @return 出队值
   */
  Mono<Message<T>> dequeue();

  /**
   * 批量出队
   *
   * @param start 开始范围
   * @param end 结束范围
   * @return 出队集合
   */
  Flux<Message<T>> dequeueBatch(int start, int end);
}
