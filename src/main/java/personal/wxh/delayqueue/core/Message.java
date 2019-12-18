package personal.wxh.delayqueue.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

/**
 * 消息对象
 *
 * @author wangxinhua
 * @since 1.0
 */
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Message<T> {

  /**
   * 构造一个不包含body的message对象
   *
   * @param id 唯一id
   * @param score 排序字段
   * @param <T> 泛型类型, ignored
   * @return 空的message
   */
  public static <T> Message<T> of(@NonNull Serializable id, @NonNull Long score) {
    return of(id, score, null);
  }

  /**
   * 用当前时间作为score | 构造一个不包含body的message对象
   *
   * @param id 唯一id
   * @param <T> 泛型类型, ignored
   * @return 空的message
   */
  public static <T> Message<T> ofNow(@NonNull Serializable id) {
    return new Message<>(id, System.currentTimeMillis(), null);
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
  @JsonCreator
  public static <T> Message<T> of(
      @JsonProperty("id") @NonNull Serializable id,
      @JsonProperty("score") @NonNull Long score,
      @JsonProperty("body") T body) {
    return new Message<>(id, score, body);
  }

  /** 唯一id */
  private Serializable id;

  /** 排序字段 */
  private double score;

  /** 自定义数据, json解析时处理如果为null则不保存 */
  private T body;
}
