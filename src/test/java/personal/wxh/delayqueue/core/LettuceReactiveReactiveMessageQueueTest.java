package personal.wxh.delayqueue.core;

import org.junit.Test;
import personal.wxh.delayqueue.BaseRedisTest;

/**
 * @author wangxinhua
 * @since 1.0
 */
public class LettuceReactiveReactiveMessageQueueTest extends BaseRedisTest {

  private LettuceReactiveReactiveMessageMessageQueue<Object> testQueue;

  public void init() {
    super.init();
    LettuceReactiveReactiveMessageMessageQueue<Object> testJobQueue =
        LettuceReactiveReactiveMessageMessageQueue.connect("testJobQueue", client);
  }

  @Test
  public void enqueue() {}

  @Test
  public void enqueueBatch() {}

  @Test
  public void dequeue() {}

  @Test
  public void dequeueBatch() {}
}
