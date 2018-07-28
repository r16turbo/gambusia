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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("exceptions")
class ExceptionsTest {

  @Test
  void testMqttConnectionRefusedException() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MqttConnectionRefusedException((byte) 0))
        .withNoCause().withMessage("Connection Accepted");

    MqttConnectionRefusedException e;
    assertThat(e = new MqttConnectionRefusedException((byte) 1))
        .hasNoCause().hasMessage("Unacceptable protocol version");
    assertEquals((byte) 1, e.returnCode());

    assertThat(e = new MqttConnectionRefusedException((byte) 2))
        .hasNoCause().hasMessage("Identifier rejected");
    assertEquals((byte) 2, e.returnCode());

    assertThat(e = new MqttConnectionRefusedException((byte) 3))
        .hasNoCause().hasMessage("Server unavailable");
    assertEquals((byte) 3, e.returnCode());

    assertThat(e = new MqttConnectionRefusedException((byte) 4))
        .hasNoCause().hasMessage("Bad user name or password");
    assertEquals((byte) 4, e.returnCode());

    assertThat(e = new MqttConnectionRefusedException((byte) 5))
        .hasNoCause().hasMessage("Not authorized");
    assertEquals((byte) 5, e.returnCode());

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MqttConnectionRefusedException((byte) 255))
        .withNoCause().withMessage("Reserved for future use");
  }

  @Test
  void testMqttDuplicateIdException() {
    MqttDuplicateIdException e;
    assertThat(e = new MqttDuplicateIdException(MqttMessageType.PUBLISH, 1))
        .hasNoCause().hasMessage("Duplicate message: type=PUBLISH, packetId=1");
    assertEquals(e.messageType(), MqttMessageType.PUBLISH);
    assertEquals(e.packetId(), 1);

    assertThat(e = new MqttDuplicateIdException(MqttMessageType.SUBSCRIBE, 2))
        .hasNoCause().hasMessage("Duplicate message: type=SUBSCRIBE, packetId=2");
    assertEquals(e.messageType(), MqttMessageType.SUBSCRIBE);
    assertEquals(e.packetId(), 2);

    assertThat(e = new MqttDuplicateIdException(MqttMessageType.UNSUBSCRIBE, 3))
        .hasNoCause().hasMessage("Duplicate message: type=UNSUBSCRIBE, packetId=3");
    assertEquals(e.messageType(), MqttMessageType.UNSUBSCRIBE);
    assertEquals(e.packetId(), 3);
  }

  @Test
  void testMqttUnexpectedIdException() {
    MqttUnexpectedIdException e;
    assertThat(e = new MqttUnexpectedIdException(MqttMessageType.PUBACK, 1))
        .hasNoCause().hasMessage("Unexpected message: type=PUBACK, packetId=1");
    assertEquals(e.messageType(), MqttMessageType.PUBACK);
    assertEquals(e.packetId(), 1);

    assertThat(e = new MqttUnexpectedIdException(MqttMessageType.SUBACK, 2))
        .hasNoCause().hasMessage("Unexpected message: type=SUBACK, packetId=2");
    assertEquals(e.messageType(), MqttMessageType.SUBACK);
    assertEquals(e.packetId(), 2);

    assertThat(e = new MqttUnexpectedIdException(MqttMessageType.UNSUBACK, 3))
        .hasNoCause().hasMessage("Unexpected message: type=UNSUBACK, packetId=3");
    assertEquals(e.messageType(), MqttMessageType.UNSUBACK);
    assertEquals(e.packetId(), 3);
  }

  @Test
  void testMqttQoSException() {
    MqttQoS qos;

    qos = MqttQoS.EXACTLY_ONCE;
    assertThat(new MqttQoSException("Unexpected QoS: " + qos.value() + " (expected: 1)"))
        .hasNoCause().hasMessage("Unexpected QoS: 2 (expected: 1)");

    qos = MqttQoS.AT_LEAST_ONCE;
    assertThat(new MqttQoSException("Unexpected QoS: " + qos.value() + " (expected: 2)"))
        .hasNoCause().hasMessage("Unexpected QoS: 1 (expected: 2)");
  }
}
