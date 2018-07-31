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

import io.netty.handler.codec.mqtt.MqttMessageType;

public class MqttUnexpectedPacketException extends MqttPacketException {

  private static final long serialVersionUID = -4106467930672049143L;

  public MqttUnexpectedPacketException(MqttMessageType type) {
    super("Unexpected packet: type=" + type, type, 0);
  }

  public MqttUnexpectedPacketException(MqttMessageType type, Throwable cause) {
    super("Unexpected packet: type=" + type, type, 0, cause);
  }

  public MqttUnexpectedPacketException(MqttMessageType type, int packetId) {
    super("Unexpected packet: type=" + type + ", packetId=" + packetId, type, packetId);
  }

  public MqttUnexpectedPacketException(MqttMessageType type, int packetId, Throwable cause) {
    super("Unexpected packet: type=" + type + ", packetId=" + packetId, type, packetId, cause);
  }
}
