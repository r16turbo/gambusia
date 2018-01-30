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

import static io.gambusia.netty.util.Args.*;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import io.gambusia.mqtt.MqttArticle;
import io.gambusia.mqtt.MqttConnectResult;
import io.gambusia.mqtt.MqttPublication;
import io.gambusia.mqtt.MqttSubscriber;
import io.gambusia.mqtt.handler.promise.MqttConnectPromise;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRecPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRelPromise;
import io.gambusia.mqtt.handler.promise.MqttPublishPromise;
import io.gambusia.mqtt.handler.promise.MqttSubscribePromise;
import io.gambusia.mqtt.handler.promise.MqttTimeLimitPromise;
import io.gambusia.mqtt.handler.promise.MqttUnsubscribePromise;
import io.gambusia.netty.util.EventExecutorTimer;
import io.gambusia.netty.util.concurrent.PromiseCanceller;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;

public class MqttClientHandler extends ChannelDuplexHandler implements MqttFixedHeaders {

  private final MqttSubscriber subscriber;
  private final long defaultTimeout;
  private final TimeUnit defaultTimeunit;
  private Timer timer;

  private Timeout keepAlive = null;
  private long lastWriteTimeNanos = 0;
  private final ChannelFutureListener writeListener = f -> {
    lastWriteTimeNanos = System.nanoTime();
  };

  private final MqttPacketId publishId = new MqttPacketId();
  private final MqttPacketId subscribeId = new MqttPacketId();
  private final MqttPacketId unsubscribeId = new MqttPacketId();

  private MqttConnectPromise connectPromise = null;
  private final Map<Integer, MqttPublishPromise> publishPromises = new ConcurrentHashMap<>();
  private final Map<Integer, Promise<Void>> releasePromises = new ConcurrentHashMap<>();
  private final Map<Integer, Promise<Void>> receivePromises = new ConcurrentHashMap<>();
  private final Map<Integer, Promise<MqttQoS[]>> subscribePromises = new ConcurrentHashMap<>();
  private final Map<Integer, Promise<Void>> unsubscribePromises = new ConcurrentHashMap<>();
  private final Queue<Promise<Void>> pingPromises = new ConcurrentLinkedQueue<>();

  public MqttClientHandler(MqttSubscriber subscriber,
      long defaultTimeout, TimeUnit defaultTimeunit) {
    this(subscriber, defaultTimeout, defaultTimeunit, null);
  }

  public MqttClientHandler(MqttSubscriber subscriber,
      long defaultTimeout, TimeUnit defaultTimeunit, Timer timer) {

    this.subscriber = checkNotNull(subscriber, "subscriber");
    this.defaultTimeout = checkPositive(defaultTimeout, "defaultTimeout");
    this.defaultTimeunit = checkNotNull(defaultTimeunit, "defaultTimeunit");

    this.timer = timer;
  }

  public boolean isConnected() {
    return connectPromise != null && connectPromise.isSuccess();
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    if (timer == null) {
      timer = new EventExecutorTimer(ctx.executor());
    }
    ctx.fireChannelRegistered();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    stopKeepAlive();
    { // mark a promises as failure.
      PromiseBreaker breaker = new PromiseBreaker(new ClosedChannelException());
      breaker.accept(connectPromise);
      publishPromises.forEach(breaker);
      releasePromises.forEach(breaker);
      receivePromises.forEach(breaker);
      subscribePromises.forEach(breaker);
      unsubscribePromises.forEach(breaker);
      pingPromises.forEach(breaker);
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {

    if (msg instanceof MqttConnectPromise) {
      write(ctx, (MqttConnectPromise) msg, promise);
    } else if (msg instanceof MqttPublishPromise) {
      write(ctx, (MqttPublishPromise) msg, promise);
    } else if (msg instanceof MqttPubRecPromise) {
      write(ctx, (MqttPubRecPromise) msg, promise);
    } else if (msg instanceof MqttPubRelPromise) {
      write(ctx, (MqttPubRelPromise) msg, promise);
    } else if (msg instanceof MqttSubscribePromise) {
      write(ctx, (MqttSubscribePromise) msg, promise);
    } else if (msg instanceof MqttUnsubscribePromise) {
      write(ctx, (MqttUnsubscribePromise) msg, promise);
    } else if (msg instanceof MqttPingPromise) {
      write(ctx, (MqttPingPromise) msg, promise);
    } else if (msg instanceof MqttMessage) {
      writeAndTouch(ctx, msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof MqttMessage) {
      MqttMessage message = (MqttMessage) msg;
      switch (message.fixedHeader().messageType()) {
        case CONNACK:
          connAckRead(ctx, (MqttConnAckMessage) msg);
          break;
        case PUBLISH:
          publishRead(ctx, (MqttPublishMessage) msg);
          break;
        case PUBACK:
          pubAckRead(ctx, message);
          break;
        case PUBREC:
          pubRecRead(ctx, message);
          break;
        case PUBREL:
          pubRelRead(ctx, message);
          break;
        case PUBCOMP:
          pubCompRead(ctx, message);
          break;
        case SUBACK:
          subAckRead(ctx, (MqttSubAckMessage) msg);
          break;
        case UNSUBACK:
          unsubAckRead(ctx, (MqttUnsubAckMessage) msg);
          break;
        case PINGRESP:
          pingRespRead(ctx, message);
          break;
        default:
          ctx.fireChannelRead(msg);
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  public void write(ChannelHandlerContext ctx, MqttConnectPromise msg, ChannelPromise channel)
      throws Exception {

    if (isConnected()) {
      msg.setFailure(new AlreadyConnectedException());
    } else {
      final MqttConnectMessage message;
      connectPromise = embedTimeLimit(msg);
      // channel(cancel, failure) -> promise
      channel.addListener(new PromiseCanceller<>(connectPromise, true));
      { // create mqtt message
        MqttArticle will = msg.getWill();
        boolean hasWill = will != null;
        message = new MqttConnectMessage(
            CONNECT_HEADER,
            new MqttConnectVariableHeader(
                msg.getProtocolName(),
                msg.getProtocolLevel(),
                msg.getUsername() != null,
                msg.getPassword() != null,
                !hasWill ? false : will.isRetain(),
                !hasWill ? 0 : will.getQoS().value(),
                hasWill,
                msg.isCleanSession(),
                msg.getKeepAlive()),
            new MqttConnectPayload(
                msg.getClientId(),
                !hasWill ? null : will.getTopic(),
                !hasWill ? null : will.getPayloadAsBytes(),
                msg.getUsername(),
                msg.getPassword()));
      }
      writeAndTouch(ctx, message, channel);
    }
  }

  public void connAckRead(ChannelHandlerContext ctx, MqttConnAckMessage msg) throws Exception {
    if (connectPromise == null) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else if (connectPromise.isSuccess()) {
      ctx.fireExceptionCaught(new AlreadyConnectedException());
    } else {
      final MqttConnAckVariableHeader variableHeader = msg.variableHeader();
      final MqttConnectReturnCode returnCode = variableHeader.connectReturnCode();
      if (returnCode == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
        connectPromise.trySuccess(new MqttConnectResult(
            variableHeader.isSessionPresent(), returnCode.byteValue()));
        startKeepAlive(ctx, connectPromise.getPingInterval(), connectPromise.getPingTimeunit());
      } else {
        connectPromise.tryFailure(new MqttConnectionRefusedException(returnCode.byteValue()));
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPublishPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final MqttArticle article = msg.getArticle();
      final int packetId;
      final boolean atMostOnce;
      switch (article.getQoS()) {
        case AT_LEAST_ONCE:
          if (msg.isDuplicate()) {
            packetId = msg.getPacketId();
          } else {
            packetId = publishId.getAndIncrement();
            msg.setPacketId(packetId);
          }
          atMostOnce = false;
          break;
        case EXACTLY_ONCE:
          if (msg.isDuplicate()) {
            packetId = msg.getPacketId();
          } else {
            packetId = publishId.getAndIncrement();
            msg.setPacketId(packetId);
          }
          atMostOnce = false;
          break;
        default:
          packetId = 0;
          atMostOnce = true;
          break;
      }
      if (!atMostOnce && publishPromises.containsKey(packetId)) {
        msg.setFailure(new MqttDuplicateIdException(MqttMessageType.PUBLISH, packetId));
      } else {
        final ByteBuf payload;
        final MqttPublishPromise promise;
        final MqttPublishMessage message;
        if (atMostOnce) {
          // QoS 0
          payload = article.getPayload();
          promise = msg;
          // channel(notify) -> promise
          channel.addListener(new PromiseNotifier<Void, ChannelFuture>(promise));
        } else {
          // QoS 1,2
          payload = article.getPayload().retain();
          promise = embedTimeLimit(msg);
          promise.addListener(f -> publishPromises.remove(packetId, promise));
          // channel(cancel, failure) -> promise
          channel.addListener(new PromiseCanceller<>(promise, true));
          publishPromises.put(packetId, promise);
        }
        { // create mqtt message
          message = new MqttPublishMessage(
              new MqttFixedHeader(MqttMessageType.PUBLISH,
                  promise.isDuplicate(),
                  article.getQoS(),
                  article.isRetain(),
                  0 // Remaining Length (don't care)
              ),
              new MqttPublishVariableHeader(article.getTopic(), packetId),
              payload);
        }
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void pubAckRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
      final MqttPublishPromise promise = publishPromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBACK, packetId));
      } else {
        final MqttArticle article = promise.getArticle();
        final MqttQoS qos = article.getQoS();
        if (qos != MqttQoS.AT_LEAST_ONCE) {
          promise.tryFailure(new MqttQoSException("invalid=" + qos.value() + ", expect=1"));
        } else {
          promise.getArticle().release();
          promise.trySuccess(null);
        }
      }
    }
  }

  public void pubRecRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
      final MqttPublishPromise promise = publishPromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBREC, packetId));
      } else {
        final MqttArticle article = promise.getArticle();
        final MqttQoS qos = article.getQoS();
        if (qos != MqttQoS.EXACTLY_ONCE) {
          promise.tryFailure(new MqttQoSException("invalid=" + qos.value() + ", expect=2"));
        } else {
          promise.getArticle().release();
          promise.trySuccess(null);
        }
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPubRelPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = msg.getPacketId();
      if (releasePromises.containsKey(packetId)) {
        msg.setFailure(new MqttDuplicateIdException(MqttMessageType.PUBREL, packetId));
      } else {
        final Promise<Void> promise = embedTimeLimit(msg);
        final MqttMessage message;
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise, true));
        // create mqtt message
        message = new MqttMessage(PUBREL_HEADER, MqttMessageIdVariableHeader.from(packetId));
        releasePromises.put(packetId, promise);
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void pubCompRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
      final Promise<Void> promise = releasePromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBCOMP, packetId));
      } else {
        promise.trySuccess(null);
      }
    }
  }

  public void publishRead(ChannelHandlerContext ctx, MqttPublishMessage msg) throws Exception {
    final MqttFixedHeader fixedHeader = msg.fixedHeader();
    final MqttPublishVariableHeader variableHeader = msg.variableHeader();
    subscriber.publicationAlived(new MqttPublication(
        fixedHeader.isDup(), fixedHeader.qosLevel(), fixedHeader.isRetain(),
        variableHeader.topicName(), variableHeader.packetId(),
        msg.payload()));
  }

  public void write(ChannelHandlerContext ctx, MqttPubRecPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = msg.getPacketId();
      if (receivePromises.containsKey(packetId)) {
        msg.setFailure(new MqttDuplicateIdException(MqttMessageType.PUBREC, packetId));
      } else {
        final Promise<Void> promise = embedTimeLimit(msg);
        final MqttMessage message;
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise, true));
        // create mqtt message
        message = new MqttMessage(PUBREC_HEADER, MqttMessageIdVariableHeader.from(packetId));
        receivePromises.put(packetId, promise);
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void pubRelRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
      final Promise<Void> promise = receivePromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.PUBREL, packetId));
      } else {
        promise.trySuccess(null);
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttSubscribePromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = subscribeId.getAndIncrement();
      if (subscribePromises.containsKey(packetId)) {
        msg.setFailure(new MqttDuplicateIdException(MqttMessageType.SUBSCRIBE, packetId));
      } else {
        final Promise<MqttQoS[]> promise = embedTimeLimit(msg);
        final MqttSubscribeMessage message;
        promise.addListener(f -> subscribePromises.remove(packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise, true));
        // create mqtt message
        message = new MqttSubscribeMessage(
            SUBSCRIBE_HEADER,
            MqttMessageIdVariableHeader.from(packetId),
            new MqttSubscribePayload(msg.getSubscriptions().stream()
                .map(subscription -> new MqttTopicSubscription(
                    subscription.getTopicFilter(),
                    subscription.getQoS()))
                .collect(Collectors.toList())));
        subscribePromises.put(packetId, promise);
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void subAckRead(ChannelHandlerContext ctx, MqttSubAckMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final MqttSubAckPayload payload = msg.payload();
      final int packetId = msg.variableHeader().messageId();
      final Promise<MqttQoS[]> promise = subscribePromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.SUBACK, packetId));
      } else {
        promise.trySuccess(payload.grantedQoSLevels().stream()
            .map(MqttQoS::valueOf).toArray(MqttQoS[]::new));
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttUnsubscribePromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = unsubscribeId.getAndIncrement();
      if (unsubscribePromises.containsKey(packetId)) {
        msg.setFailure(new MqttDuplicateIdException(MqttMessageType.UNSUBSCRIBE, packetId));
      } else {
        final Promise<Void> promise = embedTimeLimit(msg);
        final MqttUnsubscribeMessage message;
        promise.addListener(f -> unsubscribePromises.remove(packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise, true));
        // create mqtt message
        message = new MqttUnsubscribeMessage(
            UNSUBSCRIBE_HEADER,
            MqttMessageIdVariableHeader.from(packetId),
            new MqttUnsubscribePayload(msg.getTopicFilters()));
        unsubscribePromises.put(packetId, promise);
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void unsubAckRead(ChannelHandlerContext ctx, MqttUnsubAckMessage msg) throws Exception {
    if (!isConnected()) {
      ctx.fireExceptionCaught(new NotYetConnectedException());
    } else {
      final int packetId = msg.variableHeader().messageId();
      final Promise<Void> promise = unsubscribePromises.remove(packetId);
      if (promise == null) {
        ctx.fireExceptionCaught(new MqttUnknownIdException(MqttMessageType.UNSUBACK, packetId));
      } else {
        promise.trySuccess(null);
      }
    }
  }

  public void pingRespRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final Promise<Void> promise = pingPromises.poll();
    if (promise != null) {
      promise.trySuccess(null);
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPingPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final Promise<Void> promise = embedTimeLimit(msg);
      final MqttMessage message;
      // channel(cancel, failure) -> promise
      channel.addListener(new PromiseCanceller<>(promise, true));
      // create mqtt message
      message = new MqttMessage(PINGREQ_HEADER);
      pingPromises.add(promise);
      writeAndTouch(ctx, message, channel);
    }
  }

  public void startKeepAlive(ChannelHandlerContext ctx, long interval, TimeUnit unit) {
    if (keepAlive == null && interval > 0 && unit != null) {
      keepAlive = timer.newTimeout(new KeepAliveTask(ctx, interval, unit), interval, unit);
    }
  }

  public void stopKeepAlive() {
    if (keepAlive != null) {
      keepAlive.cancel();
      keepAlive = null;
    }
  }

  public void writeAndTouch(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    ctx.write(msg, promise.unvoid()).addListener(writeListener);
  }

  protected <P extends MqttTimeLimitPromise<V>, V> P embedTimeLimit(P promise) {
    final Timeout timeout;
    if (promise.isTimeLimited()) {
      timeout = timer.newTimeout(promise, promise.getTimeout(), promise.getTimeunit());
    } else {
      timeout = timer.newTimeout(promise, this.defaultTimeout, this.defaultTimeunit);
    }
    promise.addListener(f -> timeout.cancel());
    return promise;
  }

  private class KeepAliveTask implements TimerTask {

    private final ChannelHandlerContext ctx;
    private final long delay;
    private final TimeUnit unit;

    public KeepAliveTask(ChannelHandlerContext ctx, long delay, TimeUnit unit) {
      this.ctx = ctx;
      this.delay = delay;
      this.unit = unit;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
      final long nextDelayNanos = unit.toNanos(delay) - (System.nanoTime() - lastWriteTimeNanos);
      if (nextDelayNanos > 0) {
        keepAlive = timeout.timer().newTimeout(this, nextDelayNanos, TimeUnit.NANOSECONDS);
      } else {
        keepAlive = timeout.timer().newTimeout(this, delay, unit);

        Promise<Void> ping = new MqttPingPromise(ctx.executor()).addListener(f -> {
          if (!f.isSuccess()) {
            ctx.fireExceptionCaught(f.cause()).close();
          }
        });
        ctx.channel().writeAndFlush(ping).addListener(new PromiseCanceller<>(ping, true));
      }
    }
  }

  private class PromiseBreaker implements
      Consumer<Promise<?>>, BiConsumer<Integer, Promise<?>> {

    private final Throwable cause;

    public PromiseBreaker(Throwable cause) {
      this.cause = cause;
    }

    @Override
    public void accept(Promise<?> p) {
      if (p != null && !p.isDone()) {
        p.tryFailure(cause);
      }
    }

    @Override
    public void accept(Integer i, Promise<?> p) {
      if (p != null && !p.isDone()) {
        p.tryFailure(cause);
      }
    }
  }
}
