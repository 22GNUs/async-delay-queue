package personal.wxh.delayqueue.util;

/**
 * @author wangxinhua
 * @since 1.0
 */
public interface CheckedSupplier<T> {

  T get() throws Throwable;
}
