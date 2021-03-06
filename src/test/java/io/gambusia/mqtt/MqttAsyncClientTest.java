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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.gambusia.mqtt.handler.MqttClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("client")
class MqttAsyncClientTest {

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  static final String HOST = "127.0.0.1";
  static final int PORT = 1883;

  static final String PAYLOAD = "Hello World!";
  static final byte[] BYTES = PAYLOAD.getBytes(StandardCharsets.UTF_8);

  static ByteBuf payload() {
    return ByteBufAllocator.DEFAULT.directBuffer(BYTES.length).writeBytes(BYTES);
  }

  static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  @AfterAll
  static void tearDownAfterClass() throws Exception {
    WORKER_GROUP.shutdownGracefully();
  }

  private final MqttAsyncClient client = new MqttAsyncClient(10, TimeUnit.SECONDS);
  private final BlockingQueue<MqttPublication> queue = new ArrayBlockingQueue<>(8);
  private final ChannelHandler handler = new ChannelInitializer<SocketChannel>() {
    @Override
    public void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline p = ch.pipeline();
      p.addLast("mqttDecoder", new MqttDecoder());
      p.addLast("mqttEncoder", MqttEncoder.INSTANCE);
      p.addLast("mqttHandler", new MqttClientHandler((channel, msg) -> queue.put(msg)));
      p.addLast("loggingHandler", new LoggingHandler());
    }
  };

  @Nested
  class Connection {

    @BeforeEach
    void setUp() throws Exception {
      ChannelFuture f = new Bootstrap()
          .group(WORKER_GROUP).channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(handler).connect(HOST, PORT);
      client.set(f.sync().channel());
    }

    @AfterEach
    void tearDown() throws Exception {
      client.channel().close();
      client.channel().closeFuture().sync();
    }

    @Test
    void testDisconnectedOperations() throws InterruptedException {
      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        Future<?> publish = client.publish0(false, "test", Unpooled.EMPTY_BUFFER);
        assertFalse(publish.sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        assertFalse(client.received(1).sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        assertFalse(client.release(1).sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        assertFalse(client.subscribe(MqttSubscription.qos0("test")).sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        assertFalse(client.unsubscribe("test").sync().isSuccess());
      }).withNoCause();

      assertThatExceptionOfType(NotYetConnectedException.class).isThrownBy(() -> {
        assertFalse(client.ping().sync().isSuccess());
      }).withNoCause();
    }

    @Test
    void testConnect() throws InterruptedException {
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> client.connect(true, 0x0000 - 1, 60, "test"));
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> client.connect(true, 0xFFFF + 1, 60, "test"));
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> client.connect(true, 60, 0x0000 - 1, "test"));
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> client.connect(true, 60, 0xFFFF + 1, "test"));

      assertThatCode(() -> {
        MqttArticle will = new MqttArticle(MqttQoS.AT_LEAST_ONCE, false, "test/will", payload());
        String user = "user";
        byte[] pass = "pass".getBytes(StandardCharsets.UTF_8);
        Future<MqttConnectResult> future = client.connect(true, 60, 60, "test", will, user, pass);
        assertTrue(future.sync().isSuccess());

        MqttConnectResult result = future.getNow();
        assertNotNull(result);
        assertEquals(0, result.returnCode());
        assertFalse(result.isSessionPresent());
      }).doesNotThrowAnyException();

      assertThatExceptionOfType(AlreadyConnectedException.class).isThrownBy(() -> {
        assertFalse(client.connect(true, 60, 60, "test").sync().isSuccess());
      }).withNoCause();
    }

    @Test
    void testDisconnect() throws InterruptedException {
      assertTrue(client.connect(true, 60, 60, "test").sync().isSuccess());
      assertTrue(client.disconnect().sync().isSuccess());
    }

    @Test
    void testPing() throws InterruptedException {
      assertTrue(client.connect(true, 60, 60, "test").sync().isSuccess());
      assertTrue(client.ping().sync().isSuccess());
      assertTrue(client.disconnect().sync().isSuccess());
    }

    @Test
    void testStartPinger() throws InterruptedException {
      assertTrue(client.connect(true, 2, 2, "test").sync().isSuccess());
      TimeUnit.MILLISECONDS.sleep(500);
      assertTrue(client.ping().sync().isSuccess());
      TimeUnit.MILLISECONDS.sleep(3500);
      assertTrue(client.channel().isActive());
    }

    @Test
    void testStopPinger() throws InterruptedException {
      assertTrue(client.connect(true, 2, 0, "test").sync().isSuccess());
      TimeUnit.MILLISECONDS.sleep(500);
      assertTrue(client.ping().sync().isSuccess());
      TimeUnit.MILLISECONDS.sleep(3500);
      assertFalse(client.channel().isActive());
    }
  }

  abstract class TestBase {

    @BeforeEach
    @AfterEach
    void cleanUp() throws Exception {
      MqttPublication msg;
      while ((msg = queue.poll()) != null) {
        msg.release();
      }
    }

    @BeforeEach
    void setUp() throws Exception {
      ChannelFuture f = new Bootstrap()
          .group(WORKER_GROUP).channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(handler).connect(HOST, PORT);
      client.set(f.sync().channel());
      client.connect(true, 60, 60, "test").sync();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.disconnect();
      client.channel().closeFuture().sync();
    }
  }

  @Nested
  class Subscription extends TestBase {

    @Test
    void testSubscribeSuccess() throws InterruptedException {
      MqttSubscribeFuture future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isCompleteSuccess());
    }

    @Test
    void testSubscribeFailure() {
      assertThatExceptionOfType(ClosedChannelException.class).isThrownBy(() -> {
        MqttSubscribeFuture future = client.subscribe(
            MqttSubscription.qos2("test/#/2"),
            MqttSubscription.qos1("test/#/1"),
            MqttSubscription.qos0("test/#/0"));
        assertFalse(future.await().isSuccess());
        assertFalse(future.isAllSuccess());
        assertFalse(future.sync().isSuccess());
      });
    }

    @Test
    void testUnsubscribeSuccess() throws InterruptedException {
      assertTrue(client.unsubscribe("test/2", "test/1", "test/0").sync().isSuccess());
    }

    @Test
    void testUnsubscribeFailure() {
      assertThatExceptionOfType(ClosedChannelException.class).isThrownBy(() -> {
        assertFalse(client.unsubscribe("test/#/2", "test/#/1", "test/#/0").sync().isSuccess());
      });
    }
  }

  @Nested
  class PubSub extends TestBase {

    @Test
    void testPub0Sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/0/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/0", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/0/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub0Sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/0/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/1", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/0/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub0Sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/0/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/2", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/0/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub1Sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub1Sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub1Sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub2Sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub2Sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub2Sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(2, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.received(msg.packetId()).sync().isSuccess());
        assertTrue(client.complete(msg.packetId()).sync().isSuccess());
      }
    }
  }

  @Nested
  class Retransmit extends TestBase {

    @Test
    void testPub1Sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub1Sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub1Sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/1/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub2Sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      assertNull(queue.poll());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(0, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/0", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      }
    }

    @Test
    void testPub2Sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      assertNull(queue.poll());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(1, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/1", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.ack(msg.packetId()).sync().isSuccess());
      }
    }

    @Test
    void testPub2Sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      assertNull(queue.poll());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      try (MqttPublication msg = queue.poll(1, TimeUnit.SECONDS)) {
        assertNotNull(msg);
        assertFalse(msg.isDuplicate());
        assertEquals(2, msg.qos().value());
        assertFalse(msg.isRetain());
        assertEquals("test/2/2", msg.topic());
        assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

        assertTrue(client.received(msg.packetId()).sync().isSuccess());
        assertTrue(client.complete(msg.packetId()).sync().isSuccess());
      }
    }
  }
}
