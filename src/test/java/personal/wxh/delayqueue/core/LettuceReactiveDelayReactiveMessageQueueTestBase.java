package personal.wxh.delayqueue.core;

import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import personal.wxh.delayqueue.BaseRedisTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class LettuceReactiveDelayReactiveMessageQueueTestBase extends BaseRedisTest {

  private LettuceReactiveReactiveMessageDelayQueue<Object> testQueue;

  @Before
  @Override
  public void init() {
    // call super first
    super.init();
    this.testQueue =
        LettuceReactiveReactiveMessageDelayQueue.connect("testQueue", "testJobQueue", client);
  }

  @Test
  public void enqueue() {
    val step =
        Flux.range(0, 10)
            .flatMap(i -> Mono.just(Message.of(i, (long) i)))
            .flatMap(testQueue::enqueue)
            .doOnError(e -> log.error("enqueue error -> ", e))
            .onErrorStop();
    StepVerifier.create(step)
        .recordWith(ArrayList::new)
        .expectNextCount(10)
        .consumeRecordedWith(ret -> log.info("enqueue ret -> {}", ret))
        .then(testQueue::blockClearAll)
        .verifyComplete();
  }

  @Test
  public void dequeue() {
    val step =
        testQueue
            .delete()
            .then(testQueue.enqueue(Message.of(0, 0L)))
            .then(testQueue.dequeue(1))
            .doFinally(s -> testQueue.delete());
    StepVerifier.create(step)
        .consumeNextWith(
            message -> {
              Assert.assertNotNull(message);
              log.info("dequeue message -> {}", message);
            })
        .then(testQueue::blockClearAll)
        .verifyComplete();
  }

  @Test
  public void dequeueBatch() {
    val number = 10;
    val step =
        testQueue
            .delete()
            .thenMany(
                Flux.range(0, number)
                    .flatMap(i -> Mono.just(Message.of(i, (long) i)))
                    .flatMap(testQueue::enqueue)
                    .doOnError(e -> log.error("enqueue error -> ", e))
                    .onErrorStop())
            .thenMany(testQueue.dequeueBatch(number));
    StepVerifier.create(step)
        .recordWith(ArrayList::new)
        .expectNextCount(number)
        .consumeRecordedWith(ret -> log.info("dequeue batch -> {}", ret))
        .then(testQueue::blockClearAll)
        .verifyComplete();
  }
}
