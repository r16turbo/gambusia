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
package io.gambusia.mqtt;

import static io.gambusia.netty.util.Args.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import io.gambusia.mqtt.handler.MqttFixedHeaders;
import io.gambusia.mqtt.handler.promise.MqttConnectPromise;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.gambusia.mqtt.handler.promise.MqttPublishPromise;
import io.gambusia.mqtt.handler.promise.MqttSubscribePromise;
import io.gambusia.mqtt.handler.promise.MqttUnsubscribePromise;
import io.gambusia.netty.util.concurrent.PromiseCanceller;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public class MqttAsyncClient {

  private final Channel ch;

  public MqttAsyncClient(Channel ch) {
    this.ch = checkNotNull(ch, "ch");
  }

  // connect
  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit, String clientId) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        null, null, null);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit, String clientId,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        null, null, null, timeout, unit);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit,
      String clientId, String username, byte[] password) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        null, username, password);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit,
      String clientId, String username, byte[] password,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        null, username, password, timeout, unit);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit, String clientId, MqttArticle will) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        will, null, null);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit, String clientId, MqttArticle will,
      long timeout, TimeUnit unit) {
    return connect(cleanSession, keepAlive, pingInterval, pingTimeunit, clientId,
        will, null, null, timeout, unit);
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit,
      String clientId, MqttArticle will, String username, byte[] password) {
    return writeAndFlush(new MqttConnectPromise(ch.eventLoop(), "MQTT", 4,
        cleanSession, keepAlive, clientId, will, username, password, pingInterval, pingTimeunit));
  }

  public Future<MqttConnectResult> connect(boolean cleanSession,
      int keepAlive, long pingInterval, TimeUnit pingTimeunit,
      String clientId, MqttArticle will, String username, byte[] password,
      long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttConnectPromise(ch.eventLoop(), timeout, unit, "MQTT", 4,
        cleanSession, keepAlive, clientId, will, username, password, pingInterval, pingTimeunit));
  }

  // publish QoS 0
  public MqttPublishFuture publish0(boolean retain, String topic, byte[] payload) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, byte[] payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuffer payload) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuffer payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish0(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_MOST_ONCE, retain, topic, payload, timeout, unit);
  }

  // publish QoS 1
  public MqttPublishFuture publish1(boolean retain, String topic, byte[] payload) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, byte[] payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuffer payload) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuffer payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish1(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.AT_LEAST_ONCE, retain, topic, payload, timeout, unit);
  }

  // publish QoS 2
  public MqttPublishFuture publish2(boolean retain, String topic, byte[] payload) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, byte[] payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuffer payload) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuffer payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload, timeout, unit);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuf payload) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload);
  }

  public MqttPublishFuture publish2(boolean retain, String topic, ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(MqttQoS.EXACTLY_ONCE, retain, topic, payload, timeout, unit);
  }

  // publish QoS any
  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      byte[] payload) {
    return publish(new MqttArticle(qos, retain, topic, payload));
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      byte[] payload,
      long timeout, TimeUnit unit) {
    return publish(new MqttArticle(qos, retain, topic, payload), timeout, unit);
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      ByteBuffer payload) {
    return publish(new MqttArticle(qos, retain, topic, payload));
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      ByteBuffer payload,
      long timeout, TimeUnit unit) {
    return publish(new MqttArticle(qos, retain, topic, payload), timeout, unit);
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      ByteBuf payload) {
    return publish(new MqttArticle(qos, retain, topic, payload));
  }

  public MqttPublishFuture publish(MqttQoS qos, boolean retain, String topic,
      ByteBuf payload,
      long timeout, TimeUnit unit) {
    return publish(new MqttArticle(qos, retain, topic, payload), timeout, unit);
  }

  // publish base
  public MqttPublishFuture publish(MqttArticle article) {
    return writeAndFlush(new MqttPublishPromise(ch, article));
  }

  public MqttPublishFuture publish(MqttArticle article, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPublishPromise(ch, timeout, unit, article));
  }

  // publish retry
  public MqttPublishFuture publish(MqttPublishFuture future) {
    return writeAndFlush(new MqttPublishPromise(ch, future));
  }

  public MqttPublishFuture publish(MqttPublishFuture future, long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPublishPromise(ch, timeout, unit, future));
  }

  // subscribe
  public Future<MqttQoS[]> subscribe(MqttSubscription... subscriptions) {
    return writeAndFlush(new MqttSubscribePromise(ch.eventLoop(), subscriptions));
  }

  public Future<MqttQoS[]> subscribe(long timeout, TimeUnit unit,
      MqttSubscription... subscriptions) {
    return writeAndFlush(new MqttSubscribePromise(ch.eventLoop(), timeout, unit, subscriptions));
  }

  // unsubscribe
  public Future<Void> unsubscribe(String... topicFilters) {
    return writeAndFlush(new MqttUnsubscribePromise(ch.eventLoop(), topicFilters));
  }

  public Future<Void> unsubscribe(long timeout, TimeUnit unit, String... topicFilters) {
    return writeAndFlush(new MqttUnsubscribePromise(ch.eventLoop(), timeout, unit, topicFilters));
  }

  // ping
  public Future<Void> ping() {
    return writeAndFlush(new MqttPingPromise(ch.eventLoop()));
  }

  public Future<Void> ping(long timeout, TimeUnit unit) {
    return writeAndFlush(new MqttPingPromise(ch.eventLoop(), timeout, unit));
  }

  // disconnect
  public Future<Void> disconnect() {
    return ch.writeAndFlush(new MqttMessage(MqttFixedHeaders.DISCONNECT_HEADER));
  }

  // support
  public ChannelFuture closeFuture() {
    return ch.closeFuture();
  }

  protected <P extends Promise<V>, V> P writeAndFlush(P promise) {
    ch.writeAndFlush(promise).addListener(new PromiseCanceller<>(promise, true));
    return promise;
  }
}
