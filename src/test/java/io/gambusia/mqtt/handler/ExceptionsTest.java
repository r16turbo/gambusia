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
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.NoSuchElementException;
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
  void testMqttDuplicatePacketException() {
    MqttDuplicatePacketException e;
    assertThat(e = new MqttDuplicatePacketException(MqttMessageType.PUBLISH, 1))
        .hasNoCause().hasMessage("Duplicate packet: type=PUBLISH, packetId=1");
    assertEquals(MqttMessageType.PUBLISH, e.messageType());
    assertEquals(1, e.packetId());

    assertThat(e = new MqttDuplicatePacketException(MqttMessageType.SUBSCRIBE, 2))
        .hasNoCause().hasMessage("Duplicate packet: type=SUBSCRIBE, packetId=2");
    assertEquals(MqttMessageType.SUBSCRIBE, e.messageType());
    assertEquals(2, e.packetId());

    assertThat(e = new MqttDuplicatePacketException(MqttMessageType.UNSUBSCRIBE, 3))
        .hasNoCause().hasMessage("Duplicate packet: type=UNSUBSCRIBE, packetId=3");
    assertEquals(MqttMessageType.UNSUBSCRIBE, e.messageType());
    assertEquals(3, e.packetId());
  }

  @Test
  void testMqttUnexpectedPacketException() {
    final Throwable cause = new NoSuchElementException();
    MqttUnexpectedPacketException e;
    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.CONNACK))
        .hasMessage("Unexpected packet: type=CONNACK").hasNoCause();
    assertEquals(MqttMessageType.CONNACK, e.messageType());
    assertEquals(0, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.CONNACK, cause))
        .hasMessage("Unexpected packet: type=CONNACK").hasCause(cause);
    assertEquals(MqttMessageType.CONNACK, e.messageType());
    assertEquals(0, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.PUBACK, 1))
        .hasMessage("Unexpected packet: type=PUBACK, packetId=1").hasNoCause();
    assertEquals(MqttMessageType.PUBACK, e.messageType());
    assertEquals(1, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.PUBACK, 1, cause))
        .hasMessage("Unexpected packet: type=PUBACK, packetId=1").hasCause(cause);
    assertEquals(MqttMessageType.PUBACK, e.messageType());
    assertEquals(1, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.SUBACK, 2))
        .hasMessage("Unexpected packet: type=SUBACK, packetId=2").hasNoCause();
    assertEquals(MqttMessageType.SUBACK, e.messageType());
    assertEquals(2, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.SUBACK, 2, cause))
        .hasMessage("Unexpected packet: type=SUBACK, packetId=2").hasCause(cause);
    assertEquals(MqttMessageType.SUBACK, e.messageType());
    assertEquals(2, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.UNSUBACK, 3))
        .hasMessage("Unexpected packet: type=UNSUBACK, packetId=3").hasNoCause();
    assertEquals(MqttMessageType.UNSUBACK, e.messageType());
    assertEquals(3, e.packetId());

    assertThat(e = new MqttUnexpectedPacketException(MqttMessageType.UNSUBACK, 3, cause))
        .hasMessage("Unexpected packet: type=UNSUBACK, packetId=3").hasCause(cause);
    assertEquals(MqttMessageType.UNSUBACK, e.messageType());
    assertEquals(3, e.packetId());
  }

  @Test
  void testMqttQoSException() {
    MqttUnexpectedQoSException e;

    assertThat(e = new MqttUnexpectedQoSException(MqttMessageType.PUBLISH, 1, null))
        .hasNoCause().hasMessage("Unexpected packet: type=PUBLISH, packetId=1, qos=null");
    assertEquals(MqttMessageType.PUBLISH, e.messageType());
    assertEquals(1, e.packetId());
    assertNull(e.qos());

    assertThat(e = new MqttUnexpectedQoSException(MqttMessageType.PUBACK, 2, MqttQoS.AT_MOST_ONCE))
        .hasNoCause().hasMessage("Unexpected packet: type=PUBACK, packetId=2, qos=AT_MOST_ONCE");
    assertEquals(MqttMessageType.PUBACK, e.messageType());
    assertEquals(2, e.packetId());
    assertEquals(MqttQoS.AT_MOST_ONCE, e.qos());

    assertThat(e = new MqttUnexpectedQoSException(MqttMessageType.PUBREC, 3, MqttQoS.AT_LEAST_ONCE))
        .hasNoCause().hasMessage("Unexpected packet: type=PUBREC, packetId=3, qos=AT_LEAST_ONCE");
    assertEquals(MqttMessageType.PUBREC, e.messageType());
    assertEquals(3, e.packetId());
    assertEquals(MqttQoS.AT_LEAST_ONCE, e.qos());
  }
}
