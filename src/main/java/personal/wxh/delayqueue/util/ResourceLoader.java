package personal.wxh.delayqueue.util;

import lombok.NonNull;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * @author wangxinhua
 * @since 1.0
 */
public class ResourceLoader {

  /**
   * 获取classpath下的资源文件
   *
   * @param fileName 文件名
   * @return stream
   */
  public static InputStream load(@NonNull String fileName) {
    return ResourceLoader.class.getResourceAsStream(fileName);
  }

  /**
   * 读取classpath下的文件内容
   *
   * @param fileName 文件名
   * @return 文件内容字符串
   */
  public static String loadAsString(@NonNull String fileName) {
    return new BufferedReader(new InputStreamReader(load(fileName), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));
  }
}
