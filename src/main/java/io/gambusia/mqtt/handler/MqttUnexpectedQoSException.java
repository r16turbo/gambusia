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

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttUnexpectedQoSException extends MqttPacketException {

  private static final long serialVersionUID = 6352002242649565825L;

  private final MqttQoS qos;

  public MqttUnexpectedQoSException(MqttMessageType messageType, int packetId, MqttQoS qos) {
    super("Unexpected packet: type=" + messageType + ", packetId=" + packetId + ", qos=" + qos,
        messageType, packetId);
    this.qos = qos;
  }

  public MqttQoS qos() {
    return qos;
  }
}
