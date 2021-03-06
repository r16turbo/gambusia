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

package io.gambusia.mqtt;

import static io.gambusia.netty.util.Args.requireNonEmpty;
import static io.gambusia.netty.util.Args.requireNonNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.IllegalReferenceCountException;

public class MqttArticle implements ByteBufHolder {

  private final MqttQoS qos;
  private final boolean retain;
  private final String topic;
  private final ByteBuf payload;

  public MqttArticle(MqttQoS qos, boolean retain, String topic, ByteBuf payload) {
    this.qos = requireNonNull(qos, "qos");
    this.retain = retain;
    this.topic = requireNonEmpty(topic, "topic");
    this.payload = requireNonNull(payload, "payload");
  }

  public MqttQoS qos() {
    return qos;
  }

  public boolean isRetain() {
    return retain;
  }

  public String topic() {
    return topic;
  }

  public ByteBuf payload() {
    return content();
  }

  public byte[] payloadAsBytes() {
    ByteBuf payload = content().duplicate();
    byte[] bytes = new byte[payload.capacity()];
    payload.readBytes(bytes);
    return bytes;
  }

  @Override
  public int refCnt() {
    return payload.refCnt();
  }

  @Override
  public boolean release() {
    return payload.release();
  }

  @Override
  public boolean release(int decrement) {
    return payload.release(decrement);
  }

  @Override
  public ByteBuf content() {
    final int refCnt = refCnt();
    if (refCnt > 0) {
      return payload;
    }
    throw new IllegalReferenceCountException(refCnt);
  }

  @Override
  public MqttArticle copy() {
    return replace(content().copy());
  }

  @Override
  public MqttArticle duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public MqttArticle retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public MqttArticle replace(ByteBuf content) {
    return new MqttArticle(qos, retain, topic, content);
  }

  @Override
  public MqttArticle retain() {
    content().retain();
    return this;
  }

  @Override
  public MqttArticle retain(int increment) {
    content().retain(increment);
    return this;
  }

  @Override
  public MqttArticle touch() {
    content().touch();
    return this;
  }

  @Override
  public MqttArticle touch(Object hint) {
    content().touch(hint);
    return this;
  }
}
