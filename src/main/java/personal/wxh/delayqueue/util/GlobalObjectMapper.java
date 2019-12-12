package personal.wxh.delayqueue.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author wangxinhua
 * @since 1.0
 */
public class GlobalObjectMapper {
  private enum Instance {
    ;
    private static ObjectMapper mapper = new ObjectMapper();

    static {
      // 写入类型信息
      mapper.activateDefaultTypingAsProperty(
          mapper.getPolymorphicTypeValidator(), ObjectMapper.DefaultTyping.NON_FINAL, "@class");
      // 忽略空值
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
  }

  public static ObjectMapper getInstance() {
    return Instance.mapper;
  }
}
