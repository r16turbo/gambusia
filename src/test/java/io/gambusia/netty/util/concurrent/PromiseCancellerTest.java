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

package io.gambusia.netty.util.concurrent;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("promise")
class PromiseCancellerTest {

  abstract class TestBase {

    EventExecutor executor;
    Promise<String> parent;
    Promise<String> child;

    @BeforeEach
    void setUp() throws Exception {
      executor = new DefaultEventExecutor();
      parent = executor.newPromise();
      child = executor.newPromise();
      parent.addListener(new PromiseCanceller<>(child));
    }

    @AfterEach
    void tearDown() throws Exception {
      executor.shutdownGracefully();
    }
  }

  @Nested
  class FireSuccess extends TestBase {

    @Test
    void toNotDone() throws InterruptedException {
      parent.setSuccess("test");
      assertFalse(child.await(100, TimeUnit.MILLISECONDS));
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertFalse(child.isDone());
    }

    @Test
    void toSucceeded() throws InterruptedException {
      child.setSuccess("test");
      parent.setSuccess("test");
      assertTrue(child.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      child.cancel(false);
      parent.setSuccess("test");
      assertThatExceptionOfType(CancellationException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertTrue(child.isCancelled());
    }

    @Test
    void toFailed() throws InterruptedException {
      child.setFailure(new IllegalStateException());
      parent.setSuccess("test");
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }
  }

  @Nested
  class FireFailure extends TestBase {

    @Test
    void toNotDone() throws InterruptedException {
      parent.setFailure(new IllegalStateException());
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }

    @Test
    void toSucceeded() throws InterruptedException {
      child.setSuccess("test");
      parent.setFailure(new IllegalStateException());
      assertTrue(child.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      child.cancel(false);
      parent.setFailure(new IllegalStateException());
      assertThatExceptionOfType(CancellationException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertTrue(child.isCancelled());
    }

    @Test
    void toFailed() throws InterruptedException {
      child.setFailure(new IllegalStateException());
      parent.setFailure(new IllegalStateException());
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }
  }

  @Nested
  class FireCancel extends TestBase {

    @Test
    void toNotDone() throws InterruptedException {
      parent.cancel(false);
      assertThatExceptionOfType(CancellationException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertTrue(child.isCancelled());
    }

    @Test
    void toSucceeded() throws InterruptedException {
      child.setSuccess("test");
      parent.cancel(false);
      assertTrue(child.isSuccess());
    }

    @Test
    void toCancelled() throws InterruptedException {
      child.cancel(false);
      parent.cancel(false);
      assertThatExceptionOfType(CancellationException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertTrue(child.isCancelled());
    }

    @Test
    void toFailed() throws InterruptedException {
      child.setFailure(new IllegalStateException());
      parent.cancel(false);
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }

  }

  @Nested
  class FireCancelToNotCancellable extends TestBase {

    @Override
    @BeforeEach
    void setUp() throws Exception {
      executor = new DefaultEventExecutor();
      parent = executor.newPromise();
      child = new NotCancellablePromise<>(executor);
      parent.addListener(new PromiseCanceller<>(child));
    }

    @Test
    void toNotDone() throws InterruptedException {
      parent.cancel(false);
      assertThatExceptionOfType(CancellationException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }

    @Test
    void toSucceeded() throws InterruptedException {
      child.setSuccess("test");
      parent.cancel(false);
      assertTrue(child.isSuccess());
    }

    @Test
    void toFailed() throws InterruptedException {
      child.setFailure(new IllegalStateException());
      parent.cancel(false);
      assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> child.sync());
      assertFalse(child.isSuccess());
      assertFalse(child.isCancelled());
      assertTrue(child.isDone());
    }
  }

  static class NotCancellablePromise<V> extends DefaultPromise<V> {

    public NotCancellablePromise(EventExecutor executor) {
      super(executor);
    }

    @Override
    public boolean isCancellable() {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException();
    }
  }
}
