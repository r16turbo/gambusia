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

import static io.gambusia.mqtt.handler.MqttFixedHeaders.DISCONNECT_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBACK_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBCOMP_HEADER;
import static io.gambusia.netty.util.Args.requireNonNull;
import static io.gambusia.netty.util.Args.requirePositive;

import io.gambusia.mqtt.handler.MqttPinger;
import io.gambusia.mqtt.handler.promise.MqttConnectPromise;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRecPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRelPromise;
import io.gambusia.mqtt.handler.promise.MqttPublishPromise;
import io.gambusia.mqtt.handler.promise.MqttSubscribePromise;
import io.gambusia.mqtt.handler.promise.MqttUnsubscribePromise;
import io.gambusia.netty.util.concurrent.PromiseCanceller;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.TimeUnit;

public class MqttAsyncClient {

  private Channel channel;

  private final long timeout;
  private final TimeUnit unit;
  private final MqttPinger pinger;

  public MqttAsyncClient(long timeout, TimeUnit unit) {
    this(null, timeout, unit, new MqttPinger(timeout, unit));
  }

  public MqttAsyncClient(long timeout, TimeUnit unit, MqttPinger pinger) {
    this(null, timeout, unit, new MqttPinger(timeout, unit));
  }

  public MqttAsyncClient(Channel channel, long timeout, TimeUnit unit) {
    this(channel, timeout, unit, new MqttPinger(timeout, unit));
  }

  public MqttAsyncClient(Channel channel, long timeout, TimeUnit unit, MqttPinger pinger) {
    this.channel = channel;
    this.timeout = requirePositive(timeout, "timeout");
    this.unit = requireNonNull(unit, "unit");
    this.pinger = requireNonNull(pinger, "pinger");
  }

  // initializer
  public MqttAsyncClient set(Channel channel) {
    this.channel = channel;
    return this;
  }

  // accessor
  public Channel channel() {
    return channel;
  }

  public EventLoop eventLoop() {
    return channel().eventLoop();
  }

  public MqttPinger pinger() {
    return pinger;
  }

  // connect
  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, null, null, null);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, null, null, null, timeout, unit);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, String username, byte[] password) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, null, username, password);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, String username, byte[] password,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, null, username, password, timeout,
        unit);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, MqttArticle will) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, will, null, null);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, MqttArticle will,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingDelay, clientId, will, null, null, timeout, unit);
  }

  // connect base
  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, MqttArticle will, String username, byte[] password) {
    return writeAndFlush(new MqttConnectPromise(eventLoop(), timeout, unit, "MQTT", 4,
        cleanSession, keepAlive, pingDelay, pinger(), clientId, will, username, password));
  }

  public Future<MqttConnectResult> connect(boolean cleanSession, int keepAlive, int pingDelay,
      String clientId, MqttArticle will, String username, byte[] password,
      long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttConnectPromise(eventLoop(), timeout, unit, "MQTT", 4,
        cleanSession, keepAlive, pingDelay, pinger(), clientId, will, username, password));
  }

  // publish
  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic, ByteBuf payload) {
    return publish(new MqttArticle(qos, retain, topic, payload));
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(new MqttArticle(qos, retain, topic, payload), timeout, unit);
  }

  // publish base
  public MqttPublishFuture publish(MqttArticle article) {
    return writeAndFlush(new MqttPublishPromise(eventLoop(), timeout, unit, article, 0));
  }

  public MqttPublishFuture publish(MqttArticle article, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPublishPromise(eventLoop(), timeout, unit, article, 0));
  }

  // publish retry
  public MqttPublishFuture publish(MqttPublishFuture future) {
    return publish(future.article(), future.packetId());
  }

  public MqttPublishFuture publish(MqttPublishFuture future, long timeout, TimeUnit unit) {
    return publish(future.article(), future.packetId(), timeout, unit);
  }

  public MqttPublishFuture publish(MqttArticle article, int packetId) {
    return writeAndFlush(new MqttPublishPromise(eventLoop(), timeout, unit, article, packetId));
  }

  public MqttPublishFuture publish(MqttArticle article, int packetId, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPublishPromise(eventLoop(), timeout, unit, article, packetId));
  }

  // publish release
  public Future<Void> release(int packetId) {
    return writeAndFlush(new MqttPubRelPromise(eventLoop(), timeout, unit, packetId));
  }

  public Future<Void> release(int packetId, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPubRelPromise(eventLoop(), timeout, unit, packetId));
  }

  // publish ack
  public Future<Void> ack(int packetId) {
    return channel().writeAndFlush(new MqttMessage(PUBACK_HEADER,
        MqttMessageIdVariableHeader.from(packetId)));
  }

  // publish received
  public Future<Void> received(int packetId) {
    return writeAndFlush(new MqttPubRecPromise(eventLoop(), timeout, unit, packetId));
  }

  public Future<Void> received(int packetId, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPubRecPromise(eventLoop(), timeout, unit, packetId));
  }

  // publish complete
  public Future<Void> complete(int packetId) {
    return channel().writeAndFlush(new MqttMessage(PUBCOMP_HEADER,
        MqttMessageIdVariableHeader.from(packetId)));
  }

  // subscribe
  public Future<MqttQoS[]> subscribe(MqttSubscription... subscriptions) {
    return writeAndFlush(new MqttSubscribePromise(eventLoop(), timeout, unit, subscriptions));
  }

  public Future<MqttQoS[]> subscribe(long timeout, TimeUnit unit,
      MqttSubscription... subscriptions) {
    return writeAndFlush(new MqttSubscribePromise(eventLoop(), timeout, unit, subscriptions));
  }

  // unsubscribe
  public Future<Void> unsubscribe(String... topicFilters) {
    return writeAndFlush(new MqttUnsubscribePromise(eventLoop(), timeout, unit, topicFilters));
  }

  public Future<Void> unsubscribe(long timeout, TimeUnit unit, String... topicFilters) {
    return writeAndFlush(new MqttUnsubscribePromise(eventLoop(), timeout, unit, topicFilters));
  }

  // ping
  public Future<Void> ping() {
    return writeAndFlush(new MqttPingPromise(eventLoop(), timeout, unit));
  }

  public Future<Void> ping(long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPingPromise(eventLoop(), timeout, unit));
  }

  // disconnect
  public Future<Void> disconnect() {
    return channel().writeAndFlush(new MqttMessage(DISCONNECT_HEADER));
  }

  protected <P extends Promise<V>, V> P writeAndFlush(P promise) {
    channel().writeAndFlush(promise).addListener(new PromiseCanceller<>(promise));
    return promise;
  }
}
