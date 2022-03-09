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

import static io.gambusia.netty.util.Args.requireNonNull;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class PromiseCanceller<V> implements GenericFutureListener<Future<V>> {

  private static final InternalLogger logger;

  static {
    logger = InternalLoggerFactory.getInstance(PromiseCanceller.class);
  }

  private final Promise<?> promise;
  private final boolean mayInterruptIfRunning;

  public PromiseCanceller(Promise<?> promise) {
    this(promise, false);
  }

  public PromiseCanceller(Promise<?> promise, boolean mayInterruptIfRunning) {
    this.promise = requireNonNull(promise, "promise");
    this.mayInterruptIfRunning = mayInterruptIfRunning;
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    if (future.isSuccess() || promise.isDone()) {
      // noop
    } else if (future.isCancelled() && promise.isCancellable()) {
      if (!promise.cancel(mayInterruptIfRunning)) {
        logger.warn("failed to cancel promise.");
      }
    } else {
      try {
        promise.setFailure(future.cause());
      } catch (IllegalStateException e) {
        logger.warn("failed to mark a promise as failure.", e);
      }
    }
  }
}
