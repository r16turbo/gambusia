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

import io.netty.handler.codec.mqtt.MqttMessageType;

public class MqttUnknownIdException extends Exception {

  private static final long serialVersionUID = 8666052396073096437L;

  private final MqttMessageType messageType;
  private final int packetId;

  public MqttUnknownIdException(MqttMessageType messageType, int packetId) {
    super("Unknown message: type=" + messageType + ", packetId=" + packetId);
    this.messageType = messageType;
    this.packetId = packetId;
  }

  public MqttMessageType getMessageType() {
    return messageType;
  }

  public int getPacketId() {
    return packetId;
  }
}
