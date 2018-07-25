/*
 * Copyright (C) 2018 Issey Yamakoshi
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventExecutorTimerTest {

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  EventExecutor executor;
  EventExecutorTimer timer;

  @BeforeEach
  void setUp() throws Exception {
    executor = new DefaultEventExecutor();
    timer = new EventExecutorTimer(executor);
  }

  @AfterEach
  void tearDown() throws Exception {
    timer.stop();
    executor.shutdownGracefully().sync();
  }

  @Test
  void testExpire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final TimerTask task = timeout -> latch.countDown();
    final Timeout timeout = timer.newTimeout(task, 2, TimeUnit.SECONDS);
    assertEquals(timer, timeout.timer());
    assertEquals(task, timeout.task());
    assertTrue(latch.await(3, TimeUnit.SECONDS));
    assertTrue(timeout.isExpired());
    assertTrue(timer.stop().isEmpty());
  }

  @Test
  void testNotExpire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final TimerTask task = timeout -> {
      fail("This should not have run");
      latch.countDown();
    };
    final Timeout timeout = timer.newTimeout(task, 10, TimeUnit.SECONDS);
    assertEquals(timer, timeout.timer());
    assertEquals(task, timeout.task());
    assertFalse(latch.await(3, TimeUnit.SECONDS));
    assertFalse(timeout.isExpired());
    assertFalse(timer.stop().isEmpty());
  }

  @Test
  void testCancel() {
    final TimerTask task = timeout -> fail("This should not have run");
    final Timeout timeout = timer.newTimeout(task, Long.MAX_VALUE, TimeUnit.SECONDS);
    assertFalse(timeout.isCancelled());
    assertTrue(timeout.cancel());
    assertTrue(timeout.isCancelled());
    assertFalse(timeout.cancel());
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

    assertThrows(IllegalStateException.class, () -> {
      timer.newTimeout(timeout -> fail("This should not have run"), 1, TimeUnit.SECONDS);
    });
  }
}
