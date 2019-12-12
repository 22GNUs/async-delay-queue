package personal.wxh.delayqueue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.Before;

/**
 * @author wangxinhua
 * @since 1.0
 */
public abstract class BaseRedisTest {
  protected RedisClient client;

  @Before
  public void init() {
    this.client = RedisClient.create(RedisURI.Builder.redis("127.0.0.1").withPort(6379).build());
  }
}
