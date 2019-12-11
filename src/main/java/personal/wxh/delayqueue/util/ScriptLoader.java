package personal.wxh.delayqueue.util;

import io.lettuce.core.RedisClient;
import lombok.NonNull;
import lombok.val;

/**
 * @author wangxinhua
 * @since 1.0
 */
public class ScriptLoader {

  /**
   * 同步加载lua脚本, 加载完成后会关闭连接
   *
   * @param client redis客户端
   * @return 加载脚本的SHA-1值
   */
  public static String loadScript(@NonNull RedisClient client, @NonNull String fileName) {
    try (val connect = client.connect()) {
      return connect.sync().scriptLoad(ResourceLoader.loadAsString(fileName));
    } catch (Exception e) {
      throw new Error("redis连接失败", e);
    }
  }
}
