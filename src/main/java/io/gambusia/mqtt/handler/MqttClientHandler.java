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

import static io.gambusia.mqtt.handler.MqttFixedHeaders.CONNECT_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PINGREQ_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBREC_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBREL_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.SUBSCRIBE_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.UNSUBSCRIBE_HEADER;
import static io.gambusia.netty.util.Args.requireNonNull;

import io.gambusia.mqtt.MqttArticle;
import io.gambusia.mqtt.MqttConnectResult;
import io.gambusia.mqtt.MqttPublication;
import io.gambusia.mqtt.MqttSubscriber;
import io.gambusia.mqtt.MqttSubscription;
import io.gambusia.mqtt.handler.internal.Hash;
import io.gambusia.mqtt.handler.internal.Hash.MutableHash;
import io.gambusia.mqtt.handler.internal.PromiseBreaker;
import io.gambusia.mqtt.handler.internal.PromiseQueueRemover;
import io.gambusia.mqtt.handler.internal.PromiseRemover;
import io.gambusia.mqtt.handler.internal.TimeoutCanceller;
import io.gambusia.mqtt.handler.promise.MqttConnectPromise;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.gambusia.mqtt.handler.promise.MqttPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRecPromise;
import io.gambusia.mqtt.handler.promise.MqttPubRelPromise;
import io.gambusia.mqtt.handler.promise.MqttPublishPromise;
import io.gambusia.mqtt.handler.promise.MqttSubscribePromise;
import io.gambusia.mqtt.handler.promise.MqttUnsubscribePromise;
import io.gambusia.netty.util.ScheduledExecutorTimer;
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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.NotYetConnectedException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MqttClientHandler extends ChannelDuplexHandler {

  private final MqttSubscriber subscriber;
  private Timer timer;

  protected Timeout keepAlive = null;
  private long lastWriteTimeNanos = 0;
  private final ChannelFutureListener writeListener = new LastWriteTimeUpdater();

  private final MqttPacketId publishId;
  private final MqttPacketId subscribeId = new MqttPacketId();
  private final MqttPacketId unsubscribeId = new MqttPacketId();
  private final MqttUnexpectedPacketHandler unexpectedPacketHandler;

  private boolean connected = false;
  private final AtomicReference<MqttConnectPromise> connectPromise;
  private final MutableHash hash = new MutableHash(); // temporarily shared object
  private final ConcurrentMap<Hash, MqttPublishPromise> publishPromises;
  private final ConcurrentMap<Hash, Promise<Void>> releasePromises;
  private final ConcurrentMap<Hash, Promise<Void>> receivePromises;
  private final ConcurrentMap<Hash, MqttSubscribePromise> subscribePromises;
  private final ConcurrentMap<Hash, Promise<Void>> unsubscribePromises;
  private final Queue<Promise<Void>> pingPromises;

  public MqttClientHandler(MqttSubscriber subscriber) {
    this(subscriber, new MqttPacketId(), new MqttUnexpectedPacketHandler(), null);
  }

  public MqttClientHandler(MqttSubscriber subscriber, Timer timer) {
    this(subscriber, new MqttPacketId(), new MqttUnexpectedPacketHandler(), timer);
  }

  public MqttClientHandler(MqttSubscriber subscriber,
      MqttPacketId publishId, MqttUnexpectedPacketHandler handler) {
    this(subscriber, publishId, handler, null);
  }

  public MqttClientHandler(MqttSubscriber subscriber,
      MqttPacketId publishId, MqttUnexpectedPacketHandler handler, Timer timer) {
    this.subscriber = requireNonNull(subscriber, "subscriber");
    this.publishId = requireNonNull(publishId, "publishId");
    this.unexpectedPacketHandler = requireNonNull(handler, "handler");
    this.timer = timer;

    // promise container
    this.connectPromise = new AtomicReference<>();
    this.publishPromises = new ConcurrentHashMap<>();
    this.releasePromises = new ConcurrentHashMap<>();
    this.receivePromises = new ConcurrentHashMap<>();
    this.subscribePromises = new ConcurrentHashMap<>();
    this.unsubscribePromises = new ConcurrentHashMap<>();
    this.pingPromises = new ConcurrentLinkedQueue<>();
  }

  public Timer timer() {
    return timer;
  }

  public long lastWriteTimeNanos() {
    return lastWriteTimeNanos;
  }

  public boolean isConnected() {
    return connected;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    if (timer == null) {
      timer = new ScheduledExecutorTimer(ctx.executor());
    }
    ctx.fireChannelRegistered();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    stopKeepAlive();
    // mark a promises as failure.
    new PromiseBreaker(new ClosedChannelException())
        .renege(connectPromise.getAndSet(null))
        .renege(publishPromises.values())
        .renege(releasePromises.values())
        .renege(receivePromises.values())
        .renege(subscribePromises.values())
        .renege(unsubscribePromises.values())
        .renege(pingPromises);
    ctx.fireChannelInactive();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {

    if (msg instanceof MqttPromise<?>) {
      switch (((MqttPromise<?>) msg).messageType()) {
        case CONNECT:
          write(ctx, (MqttConnectPromise) msg, promise);
          break;
        case PUBLISH:
          write(ctx, (MqttPublishPromise) msg, promise);
          break;
        case PUBREC:
          write(ctx, (MqttPubRecPromise) msg, promise);
          break;
        case PUBREL:
          write(ctx, (MqttPubRelPromise) msg, promise);
          break;
        case SUBSCRIBE:
          write(ctx, (MqttSubscribePromise) msg, promise);
          break;
        case UNSUBSCRIBE:
          write(ctx, (MqttUnsubscribePromise) msg, promise);
          break;
        case PINGREQ:
          write(ctx, (MqttPingPromise) msg, promise);
          break;
        default:
          ctx.write(msg, promise);
          break;
      }
    } else if (msg instanceof MqttMessage) {
      writeAndTouch(ctx, msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  public void write(ChannelHandlerContext ctx, MqttConnectPromise msg, ChannelPromise channel)
      throws Exception {

    if (isConnected()) {
      msg.setFailure(new AlreadyConnectedException());
    } else if (!connectPromise.compareAndSet(null, msg)) {
      msg.setFailure(new ConnectionPendingException());
    } else {
      final MqttConnectPromise promise = setTimer(msg);
      final MqttConnectMessage message;
      // channel(cancel, failure) -> promise
      channel.addListener(new PromiseCanceller<>(promise));
      { // create mqtt message
        MqttArticle will = msg.will();
        boolean hasWill = will != null;
        message = new MqttConnectMessage(
            CONNECT_HEADER,
            new MqttConnectVariableHeader(
                msg.protocolName(),
                msg.protocolLevel(),
                msg.hasUsername(),
                msg.hasPassword(),
                !hasWill ? false : will.isRetain(),
                !hasWill ? 0 : will.qos().value(),
                hasWill,
                msg.isCleanSession(),
                msg.keepAlive()),
            new MqttConnectPayload(
                msg.clientId(),
                !hasWill ? null : will.topic(),
                !hasWill ? null : will.payloadAsBytes(),
                msg.username(true),
                msg.password(true)));
      }
      promise.addListener(new ConnectStateUpdater());
      startKeepAlive(ctx, promise);
      writeAndTouch(ctx, message, channel);
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPublishPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final MqttArticle article = msg.article();
      final int packetId;
      final ByteBuf payload;
      final MqttPublishPromise promise;
      final MqttPublishMessage message;
      switch (article.qos()) {
        case AT_LEAST_ONCE:
        case EXACTLY_ONCE:
          if (msg.isDuplicate()) {
            packetId = msg.packetId();
          } else {
            packetId = publishId.getAndIncrement();
            msg.packetId(packetId);
          }
          if (null != publishPromises.putIfAbsent(hash.set(packetId), msg)) {
            msg.setFailure(new MqttDuplicatePacketException(MqttMessageType.PUBLISH, packetId));
            return;
          }
          // QoS 1,2
          payload = article.payload().retain();
          promise = setTimer(msg);
          promise.addListener(new PromiseRemover<>(publishPromises, packetId, promise));
          // channel(cancel, failure) -> promise
          channel.addListener(new PromiseCanceller<>(promise));
          break;
        default:
          packetId = 0;
          // QoS 0
          payload = article.payload();
          promise = msg;
          // channel(notify) -> promise
          channel.addListener(new PromiseNotifier<Void, ChannelFuture>(promise));
          break;
      }
      // create mqtt message
      message = new MqttPublishMessage(
          new MqttFixedHeader(MqttMessageType.PUBLISH,
              promise.isDuplicate(),
              article.qos(),
              article.isRetain(),
              0 // Remaining Length (don't care)
          ),
          new MqttPublishVariableHeader(article.topic(), packetId),
          payload);
      writeAndTouch(ctx, message, channel);
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPubRecPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = msg.packetId();
      if (null != receivePromises.putIfAbsent(hash.set(packetId), msg)) {
        msg.setFailure(new MqttDuplicatePacketException(MqttMessageType.PUBREC, packetId));
      } else {
        final Promise<Void> promise = setTimer(msg);
        final MqttMessage message;
        promise.addListener(new PromiseRemover<>(receivePromises, packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise));
        // create mqtt message
        message = new MqttMessage(PUBREC_HEADER, MqttMessageIdVariableHeader.from(packetId));
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPubRelPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = msg.packetId();
      if (null != releasePromises.putIfAbsent(hash.set(packetId), msg)) {
        msg.setFailure(new MqttDuplicatePacketException(MqttMessageType.PUBREL, packetId));
      } else {
        final Promise<Void> promise = setTimer(msg);
        final MqttMessage message;
        promise.addListener(new PromiseRemover<>(releasePromises, packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise));
        // create mqtt message
        message = new MqttMessage(PUBREL_HEADER, MqttMessageIdVariableHeader.from(packetId));
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttSubscribePromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = subscribeId.getAndIncrement();
      if (null != subscribePromises.putIfAbsent(hash.set(packetId), msg)) {
        msg.setFailure(new MqttDuplicatePacketException(MqttMessageType.SUBSCRIBE, packetId));
      } else {
        final MqttSubscribePromise promise = setTimer(msg);
        final MqttSubscribeMessage message;
        promise.addListener(new PromiseRemover<>(subscribePromises, packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise));
        { // create mqtt message
          List<MqttTopicSubscription> subscriptions = new ArrayList<>(msg.subscriptions().size());
          for (MqttSubscription subscription : msg.subscriptions()) {
            subscriptions.add(new MqttTopicSubscription(
                subscription.topicFilter(), subscription.qos()));
          }
          message = new MqttSubscribeMessage(
              SUBSCRIBE_HEADER,
              MqttMessageIdVariableHeader.from(packetId),
              new MqttSubscribePayload(subscriptions));
        }
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttUnsubscribePromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final int packetId = unsubscribeId.getAndIncrement();
      if (null != unsubscribePromises.putIfAbsent(hash.set(packetId), msg)) {
        msg.setFailure(new MqttDuplicatePacketException(MqttMessageType.UNSUBSCRIBE, packetId));
      } else {
        final Promise<Void> promise = setTimer(msg);
        final MqttUnsubscribeMessage message;
        promise.addListener(new PromiseRemover<>(unsubscribePromises, packetId, promise));
        // channel(cancel, failure) -> promise
        channel.addListener(new PromiseCanceller<>(promise));
        // create mqtt message
        message = new MqttUnsubscribeMessage(
            UNSUBSCRIBE_HEADER,
            MqttMessageIdVariableHeader.from(packetId),
            new MqttUnsubscribePayload(msg.topicFilters()));
        writeAndTouch(ctx, message, channel);
      }
    }
  }

  public void write(ChannelHandlerContext ctx, MqttPingPromise msg, ChannelPromise channel)
      throws Exception {

    if (!isConnected()) {
      msg.setFailure(new NotYetConnectedException());
    } else {
      final Promise<Void> promise = setTimer(msg);
      final MqttMessage message;
      pingPromises.add(promise);
      promise.addListener(new PromiseQueueRemover<>(pingPromises));
      // channel(cancel, failure) -> promise
      channel.addListener(new PromiseCanceller<>(promise));
      // create mqtt message
      message = new MqttMessage(PINGREQ_HEADER);
      writeAndTouch(ctx, message, channel);
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
          unexpectedPacketHandler.unsupported(ctx, message);
          break;
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  public void connAckRead(ChannelHandlerContext ctx, MqttConnAckMessage msg) throws Exception {
    if (isConnected()) {
      unexpectedPacketHandler.connAck(ctx, new AlreadyConnectedException());
    } else {
      final MqttConnectPromise promise = connectPromise.getAndSet(null);
      if (promise == null) {
        unexpectedPacketHandler.connAck(ctx, new NoSuchElementException("No promise"));
      } else {
        final MqttConnAckVariableHeader variableHeader = msg.variableHeader();
        final MqttConnectReturnCode returnCode = variableHeader.connectReturnCode();
        if (returnCode == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
          MqttArticle will = promise.will();
          boolean successful = promise.trySuccess(new MqttConnectResult(
              variableHeader.isSessionPresent(), returnCode.byteValue()));
          if (will != null && (successful || promise.isSuccess())) {
            will.release();
          }
        } else {
          promise.tryFailure(new MqttConnectionRefusedException(returnCode.byteValue()));
        }
      }
    }
  }

  public void publishRead(ChannelHandlerContext ctx, MqttPublishMessage msg) throws Exception {
    final MqttFixedHeader fixedHeader = msg.fixedHeader();
    final MqttPublishVariableHeader variableHeader = msg.variableHeader();
    subscriber.arrived(ctx.channel(), new MqttPublication(
        fixedHeader.isDup(), fixedHeader.qosLevel(), fixedHeader.isRetain(),
        variableHeader.topicName(), variableHeader.packetId(),
        msg.payload()));
  }

  public void pubAckRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.pubAck(ctx, packetId, new NotYetConnectedException());
    } else {
      final MqttPublishPromise promise = publishPromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.pubAck(ctx, packetId, new NoSuchElementException("No promise"));
      } else {
        final MqttArticle article = promise.article();
        final MqttQoS qos = article.qos();
        if (qos != MqttQoS.AT_LEAST_ONCE) {
          promise.tryFailure(new MqttUnexpectedQoSException(MqttMessageType.PUBACK, packetId, qos));
        } else if (promise.trySuccess(null) || promise.isSuccess()) {
          promise.article().release();
        }
      }
    }
  }

  public void pubRecRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.pubRec(ctx, packetId, new NotYetConnectedException());
    } else {
      final MqttPublishPromise promise = publishPromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.pubRec(ctx, packetId, new NoSuchElementException("No promise"));
      } else {
        final MqttArticle article = promise.article();
        final MqttQoS qos = article.qos();
        if (qos != MqttQoS.EXACTLY_ONCE) {
          promise.tryFailure(new MqttUnexpectedQoSException(MqttMessageType.PUBREC, packetId, qos));
        } else if (promise.trySuccess(null) || promise.isSuccess()) {
          promise.article().release();
        }
      }
    }
  }

  public void pubRelRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.pubRel(ctx, packetId, new NotYetConnectedException());
    } else {
      final Promise<Void> promise = receivePromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.pubRel(ctx, packetId, new NoSuchElementException("No promise"));
      } else {
        promise.trySuccess(null);
      }
    }
  }

  public void pubCompRead(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
    final int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.pubComp(ctx, packetId, new NotYetConnectedException());
    } else {
      final Promise<Void> promise = releasePromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.pubComp(ctx, packetId, new NoSuchElementException("No promise"));
      } else {
        promise.trySuccess(null);
      }
    }
  }

  public void subAckRead(ChannelHandlerContext ctx, MqttSubAckMessage msg) throws Exception {
    final int packetId = msg.variableHeader().messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.subAck(ctx, packetId, new NotYetConnectedException());
    } else {
      final MqttSubAckPayload payload = msg.payload();
      final MqttSubscribePromise promise = subscribePromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.subAck(ctx, packetId, new NoSuchElementException("No promise"));
      } else {
        final List<Integer> results = payload.grantedQoSLevels();
        final int actual = results.size();
        final int expected = promise.subscriptions().size();
        final MqttQoS[] returnCodes = new MqttQoS[actual];
        for (int index = 0; index < actual; index++) {
          returnCodes[index] = MqttQoS.valueOf(results.get(index).intValue());
        }
        if (actual != expected) {
          promise.tryFailure(new MqttSubscribeException(
              "Number of return codes do not match: " + actual + " (expected: " + expected + ")",
              returnCodes));
        } else {
          promise.trySuccess(returnCodes);
        }
      }
    }
  }

  public void unsubAckRead(ChannelHandlerContext ctx, MqttUnsubAckMessage msg) throws Exception {
    final int packetId = msg.variableHeader().messageId();
    if (!isConnected()) {
      unexpectedPacketHandler.unsubAck(ctx, packetId, new NotYetConnectedException());
    } else {
      final Promise<Void> promise = unsubscribePromises.remove(hash.set(packetId));
      if (promise == null) {
        unexpectedPacketHandler.unsubAck(ctx, packetId, new NoSuchElementException("No promise"));
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

  public void startKeepAlive(ChannelHandlerContext ctx, MqttConnectPromise promise) {
    final long delay = promise.pingDelay();
    if (keepAlive == null && delay > 0 && !promise.isDone()) {
      promise.addListener(new KeepAliveCanceller());
      final MqttPinger pinger = promise.pinger();
      final TimeUnit unit = TimeUnit.SECONDS;
      keepAlive = timer().newTimeout(new KeepAliveTask(ctx, pinger, delay, unit), delay, unit);
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

  protected <P extends MqttPromise<V>, V> P setTimer(P promise) {
    final Timeout timeout = promise.set(timer());
    if (timeout != null) {
      promise.addListener(new TimeoutCanceller<>(timeout));
    }
    return promise;
  }

  private class LastWriteTimeUpdater implements ChannelFutureListener {
    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      lastWriteTimeNanos = System.nanoTime();
    }
  }

  private class ConnectStateUpdater implements FutureListener<MqttConnectResult> {
    @Override
    public void operationComplete(Future<MqttConnectResult> connect) throws Exception {
      connected = connect.isSuccess();
      connectPromise.set(null);
    }
  }

  protected class KeepAliveCanceller implements FutureListener<MqttConnectResult> {
    public KeepAliveCanceller() {}

    @Override
    public void operationComplete(Future<MqttConnectResult> connect) throws Exception {
      if (!connect.isSuccess()) {
        stopKeepAlive();
      }
    }
  }

  protected class KeepAliveTask implements TimerTask, FutureListener<Void> {

    private final ChannelHandlerContext ctx;
    private final MqttPinger pinger;
    private final long delay;
    private final TimeUnit unit;

    public KeepAliveTask(ChannelHandlerContext ctx, MqttPinger pinger, long delay, TimeUnit unit) {
      this.ctx = ctx;
      this.pinger = pinger;
      this.delay = delay;
      this.unit = unit;
    }

    @Override
    public final void run(Timeout timeout) throws Exception {
      check(delay, unit);
    }

    public void check(long delay, TimeUnit unit) {
      final long nextDelayNanos = unit.toNanos(delay) - (System.nanoTime() - lastWriteTimeNanos());
      if (nextDelayNanos > 0) {
        keepAlive = timer().newTimeout(this, nextDelayNanos, TimeUnit.NANOSECONDS);
      } else {
        keepAlive = timer().newTimeout(this, delay, unit);

        ping();
      }
    }

    public final void ping() {
      Promise<Void> ping = pinger.ping(ctx.executor()).addListener(this);
      ctx.channel().writeAndFlush(ping).addListener(new PromiseCanceller<>(ping));
    }

    @Override
    public final void operationComplete(Future<Void> ping) throws Exception {
      if (!ping.isSuccess()) {
        ctx.fireExceptionCaught(ping.cause()).close();
      }
    }
  }
}
