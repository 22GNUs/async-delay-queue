package personal.wxh.delayqueue.core;

import lombok.RequiredArgsConstructor;

/**
 * 基于list实现的队列
 *
 * @author wangxinhua
 * @since 1.0
 */
@RequiredArgsConstructor
public class LettuceQueue<T> {

  /** redis队列key */
  private final String key;
}
