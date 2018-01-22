/*
 * Copyright 2018 Issey Yamakoshi
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.gambusia.netty.util.concurrent;

import static io.gambusia.netty.util.Args.*;
import static io.netty.util.internal.logging.InternalLoggerFactory.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;

public class PromiseBarrier<V> implements GenericFutureListener<Future<V>> {

  private static final InternalLogger logger = getInstance(PromiseBarrier.class);

  private final AtomicInteger count = new AtomicInteger();
  private final AtomicReference<Throwable> cause = new AtomicReference<>();

  private final Promise<V> promise;
  private final V result;

  public PromiseBarrier(Promise<V> promise) {
    this(promise, null);
  }

  public PromiseBarrier(Promise<V> promise, V result) {
    this.promise = checkNotNull(promise, "promise");
    this.result = result;
  }

  public GenericFutureListener<Future<V>> add() {
    return add(1);
  }

  public GenericFutureListener<Future<V>> add(int delta) {
    count.addAndGet(delta);
    return this;
  }

  public int getNumberWaiting() {
    return count.get();
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    if (future.isSuccess()) {
      // noop
    } else if (future.isCancelled()) {
      cause.compareAndSet(null, new CancellationException());
    } else {
      cause.compareAndSet(null, future.cause());
    }
    if (count.decrementAndGet() <= 0 && !promise.isDone()) {
      Throwable cause = this.cause.get();
      if (cause == null) {
        try {
          promise.setSuccess(result);
        } catch (IllegalStateException e) {
          logger.warn("failed to mark a promise as success.", e);
        }
      } else {
        try {
          promise.setFailure(cause);
        } catch (IllegalStateException e) {
          logger.warn("failed to mark a promise as failure.", e);
        }
      }
    }
  }
}
