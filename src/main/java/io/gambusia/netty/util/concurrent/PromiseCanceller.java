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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;

public class PromiseCanceller<V> implements GenericFutureListener<Future<V>> {

  private static final InternalLogger logger = getInstance(PromiseCanceller.class);

  private final Promise<?> promise;
  private final boolean mayInterruptIfRunning;

  public PromiseCanceller(Promise<?> promise) {
    this(promise, false);
  }

  public PromiseCanceller(Promise<?> promise, boolean mayInterruptIfRunning) {
    this.promise = checkNotNull(promise, "promise");
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
