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

package io.gambusia.mqtt.handler.internal;

import io.netty.util.concurrent.Promise;

public class PromiseBreaker {

  private final Throwable cause;

  public PromiseBreaker(Throwable cause) {
    this.cause = cause;
  }

  public <P extends Promise<?>> PromiseBreaker renege(P promise) {
    if (promise != null) {
      promise.tryFailure(cause);
    }
    return this;
  }

  public <P extends Promise<?>> PromiseBreaker renege(Iterable<P> promises) {
    for (P promise : promises) {
      renege(promise);
    }
    return this;
  }
}
