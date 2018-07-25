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

package io.gambusia.mqtt.handler;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public final class MqttPacketId implements Serializable {

  private static final long serialVersionUID = -1608711409506773103L;

  public static final int MIN_VALUE = 1;
  public static final int MAX_VALUE = 65535;

  private final AtomicInteger id;

  public MqttPacketId() {
    this(MIN_VALUE);
  }

  public MqttPacketId(int initialValue) {
    if (initialValue < MIN_VALUE || initialValue > MAX_VALUE) {
      throw new IllegalArgumentException("initialValue");
    }
    this.id = new AtomicInteger(initialValue);
  }

  public int get() {
    return id.get();
  }

  public int getAndIncrement() {
    int prev;
    int next;
    do {
      next = (prev = id.get()) >= MAX_VALUE ? MIN_VALUE : prev + 1;
    } while (!id.compareAndSet(prev, next));
    return prev;
  }

  @Override
  public String toString() {
    return id.toString();
  }
}
