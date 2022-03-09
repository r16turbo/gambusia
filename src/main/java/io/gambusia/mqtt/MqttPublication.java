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

package io.gambusia.mqtt;

import static io.gambusia.netty.util.Args.requireNonEmpty;
import static io.gambusia.netty.util.Args.requireNonNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.IllegalReferenceCountException;

public class MqttPublication implements ByteBufHolder, AutoCloseable {

  private final boolean duplicate;
  private final MqttQoS qos;
  private final boolean retain;
  private final String topic;
  private final int packetId;
  private final ByteBuf payload;

  public MqttPublication(boolean duplicate, MqttQoS qos, boolean retain,
      String topic, int packetId, ByteBuf payload) {

    this.duplicate = duplicate;
    this.qos = qos;
    this.retain = retain;
    this.topic = requireNonEmpty(topic, "topic");
    this.packetId = packetId;
    this.payload = requireNonNull(payload, "payload");
  }

  public boolean isDuplicate() {
    return duplicate;
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

  public int packetId() {
    return packetId;
  }

  public ByteBuf payload() {
    return content();
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
  public MqttPublication copy() {
    return replace(content().copy());
  }

  @Override
  public MqttPublication duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public MqttPublication retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public MqttPublication replace(ByteBuf content) {
    return new MqttPublication(duplicate, qos, retain, topic, packetId, content);
  }

  @Override
  public MqttPublication retain() {
    content().retain();
    return this;
  }

  @Override
  public MqttPublication retain(int increment) {
    content().retain(increment);
    return this;
  }

  @Override
  public MqttPublication touch() {
    content().touch();
    return this;
  }

  @Override
  public MqttPublication touch(Object hint) {
    content().touch(hint);
    return this;
  }

  @Override
  public void close() throws IllegalReferenceCountException {
    release();
  }
}
