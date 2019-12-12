package personal.wxh.delayqueue.core;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import personal.wxh.delayqueue.BaseRedisTest;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class SimpleTimeBasedJobWatcherTestBase extends BaseRedisTest {

  private SimpleTimeBasedJobWatcher<Object> watcher;
  private LettuceJobReactiveQueue<Object> queue;

  public void init() {
    super.init();
    this.queue = new LettuceJobReactiveQueue<>("testQueue", "testJobQueue", client);
    this.watcher = new SimpleTimeBasedJobWatcher<>(5, TimeUnit.SECONDS, queue);
  }

  @Test
  public void watch() throws InterruptedException {}
}
