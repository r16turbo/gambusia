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

package io.gambusia.netty.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("promise")
class PromiseBarrierTest {

  abstract class TestBase {

    EventExecutor executor;
    Promise<String> parent;
    List<Promise<String>> children;
    PromiseBarrier<String> barrier;

    @BeforeEach
    void setUp() throws Exception {
      executor = new DefaultEventExecutor();
      parent = executor.newPromise();
      barrier = new PromiseBarrier<>(parent);
      children = IntStream.range(0, 1000)
          .mapToObj(i -> executor.<String>newPromise())
          .peek(p -> p.addListener(barrier.add()))
          .collect(Collectors.toList());
    }

    @AfterEach
    void tearDown() throws Exception {
      executor.shutdownGracefully();
    }
  }

  @Nested
  class FireSuccess extends TestBase implements IntConsumer {

    @Override
    public void accept(int index) {
      children.get(index).setSuccess("test");
    }

    @Test
    void toNotDone() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertTrue(parent.isSuccess());
      assertEquals(0, barrier.numberWaiting());
    }

    @Test
    void toSucceeded() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setSuccess("test");
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertTrue(parent.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.cancel(false);
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(CancellationException.class).hasNoCause();
    }

    @Test
    void toFailed() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setFailure(new IllegalStateException());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(IllegalStateException.class).hasNoCause();
    }
  }

  @Nested
  class FireFailure extends TestBase implements IntConsumer {

    @Override
    public void accept(int index) {
      Promise<String> child = children.get(index);
      if (index % 2 == 0) {
        child.setSuccess("test");
      } else {
        child.setFailure(new IllegalStateException());
      }
    }

    @Test
    void toNotDone() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(IllegalStateException.class).hasNoCause();
    }

    @Test
    void toSucceeded() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setSuccess("test");
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertTrue(parent.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.cancel(false);
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(CancellationException.class).hasNoCause();
    }

    @Test
    void toFailed() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setFailure(new IllegalStateException());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(IllegalStateException.class).hasNoCause();
    }
  }

  @Nested
  class FireCancel extends TestBase implements IntConsumer {

    @Override
    public void accept(int index) {
      Promise<String> child = children.get(index);
      if (index % 2 == 0) {
        child.setSuccess("test");
      } else {
        child.cancel(false);
      }
    }

    @Test
    void toNotDone() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(CancellationException.class).hasNoCause();
    }

    @Test
    void toSucceeded() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setSuccess("test");
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertTrue(parent.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.cancel(false);
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(CancellationException.class).hasNoCause();
    }

    @Test
    void toFailed() throws InterruptedException {
      assertEquals(children.size(), barrier.numberWaiting());
      parent.setFailure(new IllegalStateException());
      IntStream.range(0, children.size()).parallel().forEach(this);
      assertTrue(parent.await(1, TimeUnit.SECONDS));
      assertFalse(parent.isSuccess());
      assertThat(parent.cause()).isInstanceOf(IllegalStateException.class).hasNoCause();
    }
  }
}
