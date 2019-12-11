package personal.wxh.delayqueue.util;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class ResourceLoaderTest {

  @Test
  public void loadAsString() {
    val content = ResourceLoader.loadAsString("/lua/dequeue.lua");
    Assert.assertNotNull(content);
    log.info("load script success :");
    log.info(content);
  }
}
