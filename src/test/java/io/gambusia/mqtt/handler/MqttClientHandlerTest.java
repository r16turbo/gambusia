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

import static io.gambusia.mqtt.handler.MqttFixedHeaders.DISCONNECT_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PINGRESP_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBACK_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBCOMP_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBREC_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.PUBREL_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.SUBACK_HEADER;
import static io.gambusia.mqtt.handler.MqttFixedHeaders.UNSUBACK_HEADER;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.gambusia.mqtt.MqttArticle;
import io.gambusia.mqtt.MqttAsyncClient;
import io.gambusia.mqtt.MqttSubscribeFuture;
import io.gambusia.mqtt.MqttSubscriber;
import io.gambusia.mqtt.MqttSubscription;
import io.gambusia.mqtt.handler.promise.MqttConnectPromise;
import io.gambusia.mqtt.handler.promise.MqttPromise;
import io.gambusia.mqtt.handler.promise.MqttPublishPromise;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.NotYetConnectedException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("handler")
class MqttClientHandlerTest {

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  private final MqttSubscriber subscriber = (ch, msg) -> fail("This should not have run");
  private final MqttPinger pinger = new MqttPinger(10, TimeUnit.MILLISECONDS);
  private final MqttAsyncClient client = new MqttAsyncClient(10, TimeUnit.SECONDS, pinger);

  abstract class TestBase {

    EmbeddedChannel ch;

    @AfterEach
    void tearDown() throws Exception {
      ch.finishAndReleaseAll();
    }
  }

  @Nested
  class ConnectionExceptionsTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber),
          new LoggingHandler()));
    }

    @Test
    void testConnectionPendingException() {
      assertThatExceptionOfType(ConnectionPendingException.class).isThrownBy(() -> {
        Future<?> future1 = client.connect(true, 60, 60, "test");
        Future<?> future2 = client.connect(true, 60, 60, "test");

        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future1.sync().isSuccess());

        assertTrue(future2.await(1, TimeUnit.SECONDS));
        assertFalse(future2.sync().isSuccess());
      }).withNoCause();
    }

    @Test
    void testMqttConnectionRefusedException() throws InterruptedException {
      assertThatExceptionOfType(MqttConnectionRefusedException.class).isThrownBy(() -> {
        Future<?> future = client.connect(true, 60, 60, "test", 0, TimeUnit.SECONDS);
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
            .build());
        assertEquals((byte) 1, ((MqttConnectionRefusedException) future.cause()).returnCode());
        assertTrue(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(MqttConnectionRefusedException.class).isThrownBy(() -> {
        Future<?> future = client.connect(true, 60, 60, "test", 0, TimeUnit.SECONDS);
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED)
            .build());
        assertEquals((byte) 2, ((MqttConnectionRefusedException) future.cause()).returnCode());
        assertTrue(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(MqttConnectionRefusedException.class).isThrownBy(() -> {
        Future<?> future = client.connect(true, 60, 60, "test", 0, TimeUnit.SECONDS);
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            .build());
        assertEquals((byte) 3, ((MqttConnectionRefusedException) future.cause()).returnCode());
        assertTrue(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(MqttConnectionRefusedException.class).isThrownBy(() -> {
        Future<?> future = client.connect(true, 60, 60, "test", 0, TimeUnit.SECONDS);
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
            .build());
        assertEquals((byte) 4, ((MqttConnectionRefusedException) future.cause()).returnCode());
        assertTrue(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(MqttConnectionRefusedException.class).isThrownBy(() -> {
        Future<?> future = client.connect(true, 60, 60, "test", 0, TimeUnit.SECONDS);
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
            .build());
        assertEquals((byte) 5, ((MqttConnectionRefusedException) future.cause()).returnCode());
        assertTrue(future.sync().isSuccess());
      }).withNoCause();
    }
  }

  @Nested
  class MqttUnexpectedPacketExceptionTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber),
          new LoggingHandler()));
    }

    @Test
    void causedByNotYetConnectedException() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBREC_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBREC, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBREL_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBREL, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBCOMP_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBCOMP, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1), new MqttSubAckPayload(1))))
          .withMessage("Unexpected packet: type=SUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttUnsubAckMessage(UNSUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=UNSUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NotYetConnectedException.class);

      assertThatCode(() -> ch.writeInbound(new MqttMessage(PINGRESP_HEADER)))
          .doesNotThrowAnyException();
    }

    @Test
    void causedByAlreadyConnectedException() throws InterruptedException {
      assertThatCode(() -> {
        String username = "username";
        byte[] password = "password".getBytes(StandardCharsets.UTF_8);
        MqttConnectPromise promise = new MqttConnectPromise(client.eventLoop(), 0, TimeUnit.SECONDS,
            "MQTT", 4, true, 60, 60, client.pinger(), "test", null, username, password);
        {
          assertTrue(promise.hasUsername());
          assertEquals(username, promise.username());
          assertTrue(promise.hasPassword());
          assertArrayEquals(password, promise.password());
        }
        assertTrue(ch.writeAndFlush(promise).sync().isSuccess());
        {
          assertFalse(promise.hasUsername());
          assertNull(promise.username());
          assertFalse(promise.hasPassword());
          assertNull(promise.password());
        }
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(promise.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(MqttMessageBuilders.connAck()
              .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build()))
          .withMessage("Unexpected packet: type=CONNACK")
          .withCauseExactlyInstanceOf(AlreadyConnectedException.class);
    }

    @Test
    void causedByNoSuchElementException() throws InterruptedException {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(MqttMessageBuilders.connAck().build());
      }).withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatCode(() -> {
        Future<?> future = client.connect(true, 60, 60, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBREC_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBREC, packetId=1")
          .withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttMessage(PUBCOMP_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=PUBCOMP, packetId=1")
          .withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1), new MqttSubAckPayload(1))))
          .withMessage("Unexpected packet: type=SUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatExceptionOfType(MqttUnexpectedPacketException.class)
          .isThrownBy(() -> ch.writeInbound(new MqttUnsubAckMessage(UNSUBACK_HEADER,
              MqttMessageIdVariableHeader.from(1))))
          .withMessage("Unexpected packet: type=UNSUBACK, packetId=1")
          .withCauseExactlyInstanceOf(NoSuchElementException.class);

      assertThatCode(() -> {
        ch.writeInbound(new MqttMessage(PUBREL_HEADER, MqttMessageIdVariableHeader.from(1)));
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.PUBCOMP, message.fixedHeader().messageType());
        assertEquals(1, ((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
      }).doesNotThrowAnyException();
    }
  }

  @Nested
  class ClientUnsupportedMessageReceivedTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber, new MqttPacketId(), new MqttUnexpectedPacketHandler()),
          new LoggingHandler()));
    }

    @Test
    void connect() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(new MqttMessage(MqttFixedHeaders.CONNECT_HEADER));
      }).withNoCause().withMessage("Unexpected packet: type=CONNECT");
      assertFalse(ch.isActive());
    }

    @Test
    void subscribe() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(new MqttMessage(MqttFixedHeaders.SUBSCRIBE_HEADER));
      }).withNoCause().withMessage("Unexpected packet: type=SUBSCRIBE");
      assertFalse(ch.isActive());
    }

    @Test
    void unsubscribe() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(new MqttMessage(MqttFixedHeaders.UNSUBSCRIBE_HEADER));
      }).withNoCause().withMessage("Unexpected packet: type=UNSUBSCRIBE");
      assertFalse(ch.isActive());
    }

    @Test
    void ping() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(new MqttMessage(MqttFixedHeaders.PINGREQ_HEADER));
      }).withNoCause().withMessage("Unexpected packet: type=PINGREQ");
      MqttMessage message = ch.readOutbound();
      assertEquals(MqttMessageType.PINGRESP, message.fixedHeader().messageType());
    }

    @Test
    void disconnect() {
      assertThatExceptionOfType(MqttUnexpectedPacketException.class).isThrownBy(() -> {
        ch.writeInbound(new MqttMessage(MqttFixedHeaders.DISCONNECT_HEADER));
      }).withNoCause().withMessage("Unexpected packet: type=DISCONNECT");
      assertFalse(ch.isActive());
    }
  }

  @Nested
  class MqttSubscribeFutureTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(new MqttClientHandler(subscriber), new LoggingHandler()));

      assertThatCode(() -> {
        Future<?> future = client.connect(true, 60, 60, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();
    }

    @Test
    void completeSuccess() {
      MqttSubscribeFuture future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
      ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1),
          new MqttSubAckPayload(0x02, 0x01, 0x00)));
      assertTrue(future.isAllSuccess());
      assertTrue(future.isCompleteSuccess());
    }

    @Test
    void downgradedSuccess() {
      MqttSubscribeFuture future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
      ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1),
          new MqttSubAckPayload(0x02, 0x00, 0x00)));
      assertTrue(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
    }

    @Test
    void failure() {
      MqttSubscribeFuture future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
      ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1),
          new MqttSubAckPayload(0x02, 0x80, 0x00)));
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
    }

    @Test
    void cancel() throws InterruptedException {
      MqttSubscribeFuture future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
      future.cancel(false);
      assertFalse(future.isAllSuccess());
      assertFalse(future.isCompleteSuccess());
    }
  }

  @Nested
  class MqttOtherExceptionsTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber, new MqttPacketId(),
              new MqttUnexpectedPacketHandler()),
          new LoggingHandler()));
    }

    @Test
    void testMqttDuplicatePacketException() throws InterruptedException {
      assertThatCode(() -> {
        Future<?> future = client.connect(true, 60, 60, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      assertThatCode(() -> {
        for (int i = MqttPacketId.MIN_VALUE; i <= MqttPacketId.MAX_VALUE; i++) {
          client.publish1(false, "test", Unpooled.EMPTY_BUFFER, 0, TimeUnit.SECONDS);
          client.received(1, 0, TimeUnit.SECONDS);
          client.release(1, 0, TimeUnit.SECONDS);
          client.subscribe(0, TimeUnit.SECONDS, MqttSubscription.qos0("test"));
          client.unsubscribe(0, TimeUnit.SECONDS, "test");
        }
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(MqttDuplicatePacketException.class).isThrownBy(() -> {
        client.publish1(false, "test", Unpooled.EMPTY_BUFFER, 0, TimeUnit.SECONDS).sync();
      }).withNoCause().withMessage("Duplicate packet: type=PUBLISH, packetId=1");

      assertThatExceptionOfType(MqttDuplicatePacketException.class).isThrownBy(() -> {
        client.received(1, 0, TimeUnit.SECONDS).sync();
      }).withNoCause().withMessage("Duplicate packet: type=PUBREC, packetId=1");

      assertThatExceptionOfType(MqttDuplicatePacketException.class).isThrownBy(() -> {
        client.release(1, 0, TimeUnit.SECONDS).sync();
      }).withNoCause().withMessage("Duplicate packet: type=PUBREL, packetId=1");

      assertThatExceptionOfType(MqttDuplicatePacketException.class).isThrownBy(() -> {
        client.subscribe(0, TimeUnit.SECONDS, MqttSubscription.qos0("test")).sync();
      }).withNoCause().withMessage("Duplicate packet: type=SUBSCRIBE, packetId=1");

      assertThatExceptionOfType(MqttDuplicatePacketException.class).isThrownBy(() -> {
        client.unsubscribe(0, TimeUnit.SECONDS, "test").sync();
      }).withNoCause().withMessage("Duplicate packet: type=UNSUBSCRIBE, packetId=1");
    }

    @Test
    void testMqttUnexpectedQoSException() throws InterruptedException {
      assertThatCode(() -> {
        Future<?> future = client.connect(true, 60, 60, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(MqttUnexpectedQoSException.class).isThrownBy(() -> {
        Future<?> future = client.publish1(false, "test", Unpooled.EMPTY_BUFFER);
        ch.writeInbound(new MqttMessage(PUBREC_HEADER, MqttMessageIdVariableHeader.from(1)));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(MqttUnexpectedQoSException.class).isThrownBy(() -> {
        Future<?> future = client.publish2(false, "test", Unpooled.EMPTY_BUFFER);
        ch.writeInbound(new MqttMessage(PUBACK_HEADER, MqttMessageIdVariableHeader.from(2)));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();
    }
  }

  @Nested
  class TimeoutExceptionTest extends TestBase {

    Timer timer;

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber, timer = new HashedWheelTimer()),
          new LoggingHandler()));
    }

    @Override
    @AfterEach
    void tearDown() throws Exception {
      ch.finishAndReleaseAll();
      timer.stop();
    }

    @Test
    void testSendMessages() throws InterruptedException {
      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = client.connect(true, 60, 60, "test", 1, TimeUnit.NANOSECONDS)
            .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatCode(() -> {
        Future<?> future = client.connect(true, 60, 60, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        MqttPublishPromise promise =
            new MqttPublishPromise(client.eventLoop(), 1, TimeUnit.NANOSECONDS,
                new MqttArticle(MqttQoS.AT_MOST_ONCE, false, "test", Unpooled.EMPTY_BUFFER), 0);
        assertNotNull(promise.set(timer));
        CountDownLatch latch = new CountDownLatch(1);
        promise.addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(promise.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future =
            client.publish1(false, "test", Unpooled.EMPTY_BUFFER, 1, TimeUnit.NANOSECONDS)
                .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future =
            client.publish2(false, "test", Unpooled.EMPTY_BUFFER, 1, TimeUnit.NANOSECONDS)
                .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = client.received(1, 1, TimeUnit.NANOSECONDS)
            .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = client.release(1, 1, TimeUnit.NANOSECONDS)
            .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future =
            client.subscribe(1, TimeUnit.NANOSECONDS, MqttSubscription.qos0("test"))
                .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = client.unsubscribe(1, TimeUnit.NANOSECONDS, "test")
            .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = client.ping(1, TimeUnit.NANOSECONDS)
            .addListener(f -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(future.sync().isSuccess());
      }).withNoCause();
    }

    @Test
    void testPingerFailed() throws InterruptedException {
      assertThatCode(() -> {
        Future<?> future = client.connect(true, 1, 1, "test");
        ch.writeInbound(MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
        assertTrue(future.sync().isSuccess());
        MqttMessage message = ch.readOutbound();
        assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
      }).doesNotThrowAnyException();

      TimeUnit.MILLISECONDS.sleep(1200);

      MqttMessage message = ch.readOutbound();
      assertEquals(MqttMessageType.PINGREQ, message.fixedHeader().messageType());

      TimeUnit.MILLISECONDS.sleep(300);

      assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> ch.checkException());
      assertFalse(ch.isActive());
    }
  }

  @Nested
  class OtherTest extends TestBase {

    @BeforeEach
    void setUp() throws Exception {
      client.set(ch = new EmbeddedChannel(
          new MqttClientHandler(subscriber),
          new LoggingHandler()));
    }

    @Test
    void testUnhandledMessage() throws InterruptedException {
      assertTrue(ch.writeAndFlush(new DummyPromise(ch.eventLoop())).sync().isSuccess());
      assertTrue(ch.writeAndFlush("Hello World").sync().isSuccess());
      assertTrue(ch.writeAndFlush(new MqttMessage(DISCONNECT_HEADER)).sync().isSuccess());
      assertTrue(ch.writeInbound("Hello World"));
    }
  }

  static class DummyPromise extends MqttPromise<Void> {
    protected DummyPromise(EventExecutor executor) {
      super(executor, 0, TimeUnit.SECONDS);
    }

    @Override
    public final MqttMessageType messageType() {
      return MqttMessageType.PINGRESP;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
      throw new UnsupportedOperationException();
    }
  }
}
