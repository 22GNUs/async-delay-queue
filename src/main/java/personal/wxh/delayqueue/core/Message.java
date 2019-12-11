package personal.wxh.delayqueue.core;

import java.io.Serializable;
import lombok.*;

/**
 * 消息对象
 *
 * @author wangxinhua
 * @since 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Message<T> {

  /**
   * 构造一个不包含body的message对象
   *
   * @param id 唯一id
   * @param score 排序字段
   * @param <T> 泛型类型, ignored
   * @return 空的message
   */
  public static <T> Message<T> of(Serializable id, Long score) {
    return new Message<>(id, score);
  }

  /**
   * 构造一个包含body的message对象
   *
   * @param id 唯一id
   * @param score 排序字段
   * @param body 自定义内容
   * @param <T> body 泛型类型
   * @return 包含自定义信息的message对象
   */
  public static <T> Message<T> withBody(Serializable id, Long score, @NonNull T body) {
    return new Message<>(id, score, body);
  }

  /** 唯一id */
  private @NonNull Serializable id;

  /** 排序字段 */
  private @NonNull Long score;

  /** 自定义数据, json解析时处理如果为null则不保存 */
  private T body;
}
