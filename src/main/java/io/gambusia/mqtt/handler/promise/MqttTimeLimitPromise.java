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

import java.util.concurrent.TimeUnit;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;

public abstract class MqttTimeLimitPromise<V> extends DefaultPromise<V> implements TimerTask {

  private final boolean limited;
  private final long timeout;
  private final TimeUnit timeunit;

  protected MqttTimeLimitPromise(EventExecutor executor, long timeout, TimeUnit timeunit) {
    super(executor);
    this.limited = timeout > 0 && timeunit != null;
    this.timeout = limited ? timeout : 0;
    this.timeunit = limited ? timeunit : null;
  }

  public boolean isTimeLimited() {
    return limited;
  }

  public long timeout() {
    return timeout;
  }

  public TimeUnit timeunit() {
    return timeunit;
  }
}
