package personal.wxh.delayqueue.util;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import personal.wxh.delayqueue.BaseRedisTest;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class ScriptLoaderBaseRedisTest extends BaseRedisTest {

  @Test
  public void loadScript() {
    val file = "/lua/dequeue.lua";
    val sha1 = ScriptLoader.loadScript(client, file);
    Assert.assertNotNull(sha1);
    log.info("file {} sha1 -> {}", file, sha1);
  }
}
