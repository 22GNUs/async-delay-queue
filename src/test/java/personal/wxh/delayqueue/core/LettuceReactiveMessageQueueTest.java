package personal.wxh.delayqueue.core;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import personal.wxh.delayqueue.BaseRedisTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * @author wangxinhua
 * @since 1.0
 */
@Slf4j
public class LettuceReactiveMessageQueueTest extends BaseRedisTest {

  private LettuceReactiveMessageQueue<Object> testQueue;

  public void init() {
    super.init();
    testQueue = LettuceReactiveMessageQueue.connect("testJobQueue", client);
  }

  @Test
  public void enqueue() {
    val number = 10;
    val step =
        testQueue
            .delete()
            .thenMany(Flux.range(0, number).map(Message::ofNow).flatMap(testQueue::enqueue));
    StepVerifier.create(step).expectNextCount(number).then(testQueue::syncDelete).verifyComplete();
  }

  @Test
  public void enqueueBatch() {
    val number = 10;
    val lst = IntStream.range(0, number).mapToObj(Message::ofNow).collect(Collectors.toList());
    val step = testQueue.delete().thenMany(testQueue.enqueueBatch(lst));
    StepVerifier.create(step)
        .expectNext(Long.valueOf(number))
        .then(testQueue::syncDelete)
        .verifyComplete();
  }

  @Test
  public void dequeue() {
    val number = 10;
    val step =
        testQueue
            .delete()
            .thenMany(Flux.range(0, number).map(Message::ofNow).flatMap(testQueue::enqueue))
            .thenMany(Flux.range(0, number).flatMap(__ -> testQueue.dequeue()));
    StepVerifier.create(step)
        .recordWith(ArrayList::new)
        .expectNextCount(number)
        .then(testQueue::syncDelete)
        .consumeRecordedWith(messages -> Assert.assertEquals(messages.size(), number))
        .verifyComplete();
  }
}
