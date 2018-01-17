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
package io.gambusia.mqtt.handler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntBinaryOperator;

public final class MqttPacketId {

  private final AtomicInteger id;

  public MqttPacketId() {
    this(1);
  }

  public MqttPacketId(int initialValue) {
    if (initialValue < 1 || initialValue > 0x0000ffff) {
      throw new IllegalArgumentException("initialValue");
    }
    this.id = new AtomicInteger(initialValue);
  }

  public int get() {
    return id.get();
  }

  public int getAndIncrement() {
    return id.getAndAccumulate(1, ACCUMULATOR);
  }

  private static final IntBinaryOperator ACCUMULATOR = (now, delta) -> {
    int increment = now + delta & 0x0000ffff;
    return increment == 0 ? 1 : increment;
  };
}
