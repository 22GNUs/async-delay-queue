package personal.wxh.delayqueue.util;

import static personal.wxh.delayqueue.util.Exceptions.checked;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import lombok.NonNull;
import lombok.val;
import reactor.core.publisher.Mono;

/**
 * 读写json, 类似一个trait
 *
 * @author wangxinhua
 * @since 1.0
 */
public interface ReactiveJsonFormatter<T> {

  /**
   * 获取内部的objectMapper
   *
   * @return objectMapper
   */
  ObjectMapper getObjectMapper();

  /**
   * 要序列化的类型
   *
   * @return 类型class
   */
  Class<T> getMetaClazz();

  /**
   * 读取json，支持复合类型
   *
   * @param json 目标json
   * @param parametricClazz 包装类型class
   * @param <R> 包装类型泛型
   * @return 异步读取结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  default <R> Mono<T> readValueParametric(@NonNull String json, @NonNull Class<R> parametricClazz) {
    val type =
        getObjectMapper().getTypeFactory().constructParametricType(parametricClazz, getMetaClazz());
    return Mono.fromSupplier(checked(() -> getObjectMapper().readValue(json, type)));
  }

  /**
   * 科里化版本的 {@link this#readValueParametric(String, Class)}
   *
   * @param parametricClazz 包装类型class
   * @param <R> 包装类型泛型
   * @return 异步读取结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  default <R> Function<String, Mono<T>> readValueParametric(Class<R> parametricClazz) {
    return json -> readValueParametric(json, parametricClazz);
  }

  /**
   * 读取json
   *
   * @param json 目标json
   * @return 异步读取结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  default Mono<T> readValue(@NonNull String json) {
    return Mono.fromSupplier(checked(() -> getObjectMapper().readValue(json, getMetaClazz())));
  }

  /**
   * 写入json
   *
   * @param value 目标对象
   * @return 异步写入结果 | 异常会被包装为 {@code RuntimeException}
   * @see Exceptions#checked(CheckedSupplier)
   */
  default Mono<String> writeValue(@NonNull Object value) {
    return Mono.fromSupplier(checked(() -> getObjectMapper().writeValueAsString(value)));
  }
}
