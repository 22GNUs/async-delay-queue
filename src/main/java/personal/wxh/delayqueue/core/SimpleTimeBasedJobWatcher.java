package personal.wxh.delayqueue.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * 持续监听队列, 搬运数据到jobQueue
 *
 * @author wangxinhua
 * @since 1.0
 */
@RequiredArgsConstructor
@Slf4j
public class SimpleTimeBasedJobWatcher<T> {

  private final int delay;
  private final TimeUnit unit;

  /** 单线程监听 */
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  private ScheduledFuture<?> scheduledFuture;

  private final LettuceJobReactiveQueue<T> watchQueue;

  /** 开始监听 */
  public void watch() {
    scheduledFuture =
        service.schedule(
            () -> {
              watchQueue
                  .dequeueBatch(System.currentTimeMillis())
                  .subscribe(
                      msg ->
                          log.info(
                              "transfer message to job queue -> {}", watchQueue.getJobQueueKey()));
            },
            delay,
            unit);
  }

  /**
   * 开始监听, 到时间后关闭
   *
   * @param timeout 延迟时间
   * @param unit 时间单位
   * @throws InterruptedException 中断异常
   */
  public void watch(int timeout, TimeUnit unit) throws InterruptedException {
    try {
      watch();
      service.awaitTermination(timeout, unit);
    } finally {
      shutdown();
    }
  }

  /** 关闭服务 */
  public void shutdown() {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
    service.shutdown();
  }

  public static void main(String[] args) throws InterruptedException {
    val queue =
        new LettuceJobReactiveQueue<>(
            "testQueue",
            "testJobQueue",
            RedisClient.create(RedisURI.builder().withHost("127.0.0.1").withPort(6379).build()));
    new Thread(
            () -> {
              for (int i = 0; i < 10; i++) {
                queue
                    .enqueue(Message.ofNow(i))
                    .subscribe(message -> log.info("enqueue message -> {}", message));
              }
            })
        .start();
    new SimpleTimeBasedJobWatcher<>(5, TimeUnit.SECONDS, queue).watch(30, TimeUnit.SECONDS);
  }
}
