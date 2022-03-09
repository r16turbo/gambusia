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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("packetId")
class MqttPacketIdTest {

  @Test
  void test() {
    assertIllegalArgumentException(MqttPacketId::new, Integer.MIN_VALUE);
    assertIllegalArgumentException(MqttPacketId::new, MqttPacketId.MIN_VALUE - 1);

    assertDoesNotThrow(() -> {
      MqttPacketId id = new MqttPacketId();
      assertGetAndIncrement(MqttPacketId.MIN_VALUE, id);
      assertGetAndIncrement(2, id);
      assertGetAndIncrement(3, id);
    });
    assertDoesNotThrow(() -> {
      MqttPacketId id = new MqttPacketId(MqttPacketId.MIN_VALUE);
      assertGetAndIncrement(1, id);
      assertGetAndIncrement(2, id);
      assertGetAndIncrement(3, id);
    });
    assertDoesNotThrow(() -> {
      MqttPacketId id = new MqttPacketId(MqttPacketId.MAX_VALUE);
      assertGetAndIncrement(MqttPacketId.MAX_VALUE, id);
      assertGetAndIncrement(1, id);
      assertGetAndIncrement(2, id);
    });

    assertIllegalArgumentException(MqttPacketId::new, MqttPacketId.MAX_VALUE + 1);
    assertIllegalArgumentException(MqttPacketId::new, Integer.MAX_VALUE);
  }

  interface Builder<V> {
    V build(int i);
  }

  static <V> void assertIllegalArgumentException(Builder<V> builder, int arg) {
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> builder.build(arg));
  }

  static <V> void assertGetAndIncrement(int expected, MqttPacketId id) {
    assertEquals(expected, id.get());
    assertEquals(String.valueOf(expected), id.toString());
    assertEquals(expected, id.getAndIncrement());
  }
}
