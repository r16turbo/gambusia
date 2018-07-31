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

import static io.gambusia.mqtt.handler.MqttFixedHeaders.PINGRESP_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBCOMP_HEADER;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.NoSuchElementException;

public class MqttUnexpectedPacketHandler {

  public void connAck(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.CONNACK, cause));
  }

  public void pubAck(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.PUBACK, packetId, cause));
  }

  public void pubRec(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.PUBREC, packetId, cause));
  }

  public void pubRel(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    if (cause instanceof NoSuchElementException) {
      ctx.channel().writeAndFlush(
          new MqttMessage(PUBCOMP_HEADER, MqttMessageIdVariableHeader.from(packetId)));
    } else {
      ctx.fireExceptionCaught(
          new MqttUnexpectedPacketException(MqttMessageType.PUBREL, packetId, cause));
    }
  }

  public void pubComp(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.PUBCOMP, packetId, cause));
  }

  public void subAck(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.SUBACK, packetId, cause));
  }

  public void unsubAck(ChannelHandlerContext ctx, int packetId, Throwable cause) throws Exception {
    ctx.fireExceptionCaught(
        new MqttUnexpectedPacketException(MqttMessageType.UNSUBACK, packetId, cause));
  }

  public void unsupported(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final MqttMessageType type = msg.fixedHeader().messageType();
    ctx.fireExceptionCaught(new MqttUnexpectedPacketException(type));
    if (type == MqttMessageType.PINGREQ) {
      ctx.channel().writeAndFlush(new MqttMessage(PINGRESP_HEADER));
    } else {
      ctx.close();
    }
  }
}
