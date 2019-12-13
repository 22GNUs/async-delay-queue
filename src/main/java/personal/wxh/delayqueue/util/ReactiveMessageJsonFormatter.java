package personal.wxh.delayqueue.util;

import static personal.wxh.delayqueue.util.Exceptions.checked;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import personal.wxh.delayqueue.core.Message;
import reactor.core.publisher.Mono;

/**
 * 读写json
 *
 * @author wangxinhua
 * @since 1.0
 */
@RequiredArgsConstructor
public class ReactiveMessageJsonFormatter<T> {

  private final ObjectMapper objectMapper;
  private final Class<T> metaClass;

  /**
   * 读取json，支持复合类型
   *
   * @param json 目标json
   * @return 异步读取结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  public Mono<Message<T>> readValue(@NonNull String json) {
    val type = objectMapper.getTypeFactory().constructParametricType(Message.class, metaClass);
    return Mono.fromSupplier(checked(() -> objectMapper.readValue(json, type)));
  }

  /**
   * 写入json
   *
   * @param value 目标对象
   * @return 异步写入结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  public Mono<String> writeValue(@NonNull Object value) {
    return Mono.fromSupplier(checked(() -> objectMapper.writeValueAsString(value)));
  }
}
