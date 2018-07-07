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

package io.gambusia.mqtt.handler.promise;

import static io.gambusia.netty.util.Args.*;

import java.util.concurrent.TimeUnit;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;

public abstract class MqttPromise<V> extends DefaultPromise<V> implements TimerTask {

  private final long timeout;
  private final TimeUnit unit;

  protected MqttPromise(EventExecutor executor, long timeout, TimeUnit unit) {
    super(executor);
    this.timeout = checkPositive(timeout, "timeout");
    this.unit = checkNotNull(unit, "unit");
  }

  public Timeout set(Timer timer) {
    return timer.newTimeout(this, timeout, unit);
  }
}
