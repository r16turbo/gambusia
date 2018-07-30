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
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.NotYetConnectedException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("handler")
class MqttClientHandlerTest {

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  private final MqttPinger pinger = new MqttPinger(10, TimeUnit.MILLISECONDS);
  private final MqttAsyncClient client = new MqttAsyncClient(10, TimeUnit.SECONDS, pinger);

  Timer timer;
  EmbeddedChannel ch;

  @BeforeEach
  void setUp() throws Exception {
    timer = new HashedWheelTimer();
    ch = new EmbeddedChannel(
        new MqttClientHandler((ch, msg) -> fail("This should not have run"), timer),
        new LoggingHandler());
    client.set(ch);
  }

  @AfterEach
  void tearDown() throws Exception {
    ch.finishAndReleaseAll();
    timer.stop();
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

  @Test
  void testUnhandledMessage() throws InterruptedException {
    assertTrue(ch.writeAndFlush(new MqttPromise<Void>(ch.eventLoop(), 1, TimeUnit.SECONDS) {
      @Override
      public MqttMessageType messageType() {
        return MqttMessageType.PINGRESP;
      }

      @Override
      public void run(Timeout timeout) throws Exception {}
    }).sync().isSuccess());
    assertTrue(ch.writeAndFlush("Hello World").sync().isSuccess());
    assertTrue(ch.writeAndFlush(new MqttMessage(DISCONNECT_HEADER)).sync().isSuccess());
    assertTrue(ch.writeInbound("Hello World"));
    assertTrue(ch.writeInbound(new MqttMessage(DISCONNECT_HEADER)));
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

  @Test
  void testAlreadyConnectedException() throws InterruptedException {
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

    assertThatExceptionOfType(AlreadyConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(MqttMessageBuilders.connAck()
          .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
    }).withNoCause();
  }

  @Test
  void testNotYetConnectedException() {
    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(MqttMessageBuilders.connAck().build());
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBACK_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBREC_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBREL_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBCOMP_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1), new MqttSubAckPayload(1)));
    }).withNoCause();

    assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttUnsubAckMessage(UNSUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatCode(() -> {
      ch.writeInbound(new MqttMessage(PINGRESP_HEADER));
    }).doesNotThrowAnyException();
  }

  @Test
  void testTimeoutException() throws InterruptedException {
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
      Future<?> future = client.subscribe(1, TimeUnit.NANOSECONDS, MqttSubscription.qos0("test"))
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
  void testMqttUnexpectedIdException() throws InterruptedException {
    assertThatCode(() -> {
      Future<?> future = client.connect(true, 60, 60, "test");
      ch.writeInbound(MqttMessageBuilders.connAck()
          .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build());
      assertTrue(future.sync().isSuccess());
      MqttMessage message = ch.readOutbound();
      assertEquals(MqttMessageType.CONNECT, message.fixedHeader().messageType());
    }).doesNotThrowAnyException();

    assertThatExceptionOfType(MqttUnexpectedIdException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBACK_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(MqttUnexpectedIdException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBREC_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(MqttUnexpectedIdException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttMessage(PUBCOMP_HEADER, MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatExceptionOfType(MqttUnexpectedIdException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttSubAckMessage(SUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1), new MqttSubAckPayload(1)));
    }).withNoCause();

    assertThatExceptionOfType(MqttUnexpectedIdException.class).isThrownBy(() -> {
      ch.writeInbound(new MqttUnsubAckMessage(UNSUBACK_HEADER,
          MqttMessageIdVariableHeader.from(1)));
    }).withNoCause();

    assertThatCode(() -> {
      ch.writeInbound(new MqttMessage(PUBREL_HEADER, MqttMessageIdVariableHeader.from(1)));
      MqttMessage message = ch.readOutbound();
      assertEquals(MqttMessageType.PUBCOMP, message.fixedHeader().messageType());
      assertEquals(1, ((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
    }).doesNotThrowAnyException();
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
