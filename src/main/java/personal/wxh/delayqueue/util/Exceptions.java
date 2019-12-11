package personal.wxh.delayqueue.util;

import java.util.function.Supplier;
import lombok.NonNull;

/**
 * @author wangxinhua
 * @since 1.0
 */
public class Exceptions {

  /**
   * 包装checkedException, 转换为RuntimeException
   *
   * @param supplier 可能抛出任何异常的supplier
   * @param <T> supplier内部元素类型
   * @return 可能抛出 {@code RuntimeException} 的supplier
   */
  public static <T> Supplier<T> checked(@NonNull CheckedSupplier<T> supplier) {
    return () -> {
      try {
        return supplier.get();
      } catch (Throwable throwable) {
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
      }
    };
  }
}
