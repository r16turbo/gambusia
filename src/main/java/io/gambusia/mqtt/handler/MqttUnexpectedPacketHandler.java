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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessageType;

public class MqttUnexpectedPacketHandler {

  public void pubAckRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBACK, packetId));
  }

  public void pubRecRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBREC, packetId));
  }

  public void pubRelRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBREL, packetId));
  }

  public void pubCompRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBCOMP, packetId));
  }

  public void subAckRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.SUBACK, packetId));
  }

  public void unsubAckRead(ChannelHandlerContext ctx, int packetId) throws Exception {
    ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.UNSUBACK, packetId));
  }
}
