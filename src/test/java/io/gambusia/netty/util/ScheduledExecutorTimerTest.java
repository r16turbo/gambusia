/*
 * Copyright 2018 Issey Yamakoshi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gambusia.netty.util;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("timer")
class ScheduledExecutorTimerTest {

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  Timer timer;

  @Nested
  class JavaScheduledExecutorTest extends TestBase {

    ScheduledExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
      executor = Executors.newSingleThreadScheduledExecutor();
      timer = new ScheduledExecutorTimer(executor);
    }

    @AfterEach
    void tearDown() throws Exception {
      timer.stop();
      executor.shutdown();
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  @Nested
  class NettyScheduledExecutorTest extends TestBase {

    EventExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
      executor = new DefaultEventExecutor();
      timer = new ScheduledExecutorTimer(executor);
    }

    @AfterEach
    void tearDown() throws Exception {
      timer.stop();
      executor.shutdownGracefully();
    }
  }

  abstract class TestBase {

    @Test
    void testExpire() throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(1);
      final TimerTask task = timeout -> latch.countDown();
      final Timeout timeout = timer.newTimeout(task, 1, TimeUnit.MILLISECONDS);
      assertEquals(timer, timeout.timer());
      assertEquals(task, timeout.task());
      assertTrue(latch.await(1, TimeUnit.SECONDS));
      assertTrue(timeout.isExpired());
      assertTrue(timer.stop().isEmpty());
    }

    @Test
    void testExpireAndThrown() throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(1);
      final TimerTask task = timeout -> {
        latch.countDown();
        throw new IllegalStateException("Exception for the exam");
      };
      final Timeout timeout = timer.newTimeout(task, 1, TimeUnit.MILLISECONDS);
      assertEquals(timer, timeout.timer());
      assertEquals(task, timeout.task());
      assertTrue(latch.await(1, TimeUnit.SECONDS));
      assertTrue(timeout.isExpired());
      assertTrue(timer.stop().isEmpty());
    }

    @Test
    void testNotExpire() throws InterruptedException {
      final TimerTask task = timeout -> fail("This should not have run");
      final Timeout timeout = timer.newTimeout(task, 100, TimeUnit.MILLISECONDS);
      assertEquals(timer, timeout.timer());
      assertEquals(task, timeout.task());
      assertFalse(timeout.isExpired());
      assertFalse(timer.stop().isEmpty());
      assertTrue(timer.stop().isEmpty());
    }

    @Test
    void testCancel() throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(1);
      final TimerTask task = timeout -> fail("This should not have run");
      final Timeout timeout = timer.newTimeout(task, 100, TimeUnit.MILLISECONDS);
      assertFalse(timeout.isExpired());
      assertFalse(timeout.isCancelled());
      assertTrue(timeout.cancel());
      assertTrue(timeout.isCancelled());
      assertFalse(timeout.cancel());
      assertFalse(latch.await(200, TimeUnit.MILLISECONDS));
      assertFalse(timeout.isExpired());
      assertTrue(timer.stop().isEmpty());
    }

    @Test
    void testStoppedTimer() throws InterruptedException {
      final int count = 10;
      final CountDownLatch latch = new CountDownLatch(count);
      for (int i = 0; i < count; i++) {
        timer.newTimeout(timeout -> latch.countDown(), 1, TimeUnit.MILLISECONDS);
      }
      assertTrue(latch.await(1, TimeUnit.SECONDS));
      assertTrue(timer.stop().isEmpty());

      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
        timer.newTimeout(timeout -> fail("This should not have run"), 1, TimeUnit.SECONDS);
      });
    }
  }
}
