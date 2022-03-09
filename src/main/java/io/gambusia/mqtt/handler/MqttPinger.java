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

package io.gambusia.mqtt.handler;

import static io.gambusia.netty.util.Args.requireNonNull;
import static io.gambusia.netty.util.Args.requirePositive;

import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.TimeUnit;

public class MqttPinger {

  protected final long timeout;
  protected final TimeUnit unit;

  public MqttPinger(long timeout, TimeUnit unit) {
    this.timeout = requirePositive(timeout, "timeout");
    this.unit = requireNonNull(unit, "unit");
  }

  protected MqttPingPromise ping(EventExecutor executor) {
    return new MqttPingPromise(executor, timeout, unit);
  }
}
