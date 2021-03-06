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

package io.gambusia.mqtt.handler.promise;

import static io.gambusia.netty.util.Args.requireNonNegative;
import static io.gambusia.netty.util.Args.requireNonNull;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.TimeUnit;

public abstract class MqttPromise<V> extends DefaultPromise<V> implements TimerTask {

  protected final long timeout;
  protected final TimeUnit unit;

  protected MqttPromise(EventExecutor executor, long timeout, TimeUnit unit) {
    super(executor);
    this.timeout = requireNonNegative(timeout, "timeout");
    this.unit = requireNonNull(unit, "unit");
  }

  public abstract MqttMessageType messageType();

  public Timeout set(Timer timer) {
    if (timeout > 0) {
      return timer.newTimeout(this, timeout, unit);
    } else {
      return null;
    }
  }
}
