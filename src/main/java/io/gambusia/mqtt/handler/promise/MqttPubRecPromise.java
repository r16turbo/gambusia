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
import java.util.concurrent.TimeoutException;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;

public class MqttPubRecPromise extends MqttPromise<Void> {

  private final int packetId;

  public MqttPubRecPromise(EventExecutor executor, long timeout, TimeUnit unit, int packetId) {
    super(executor, timeout, unit);
    this.packetId = packetId;
  }

  public int packetId() {
    return packetId;
  }

  @Override
  public void run(Timeout timeout) {
    tryFailure(new TimeoutException("PUBREL"));
  }
}
