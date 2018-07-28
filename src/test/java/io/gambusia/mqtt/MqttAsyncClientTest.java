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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
  @Tag("connection")
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
    @DisplayName("disconnected")
    void disconnected() throws InterruptedException {
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
    @DisplayName("connect")
    void connect() throws InterruptedException {
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
    @DisplayName("disconnect")
    void disconnect() throws InterruptedException {
      assertTrue(client.connect(true, 60, 60, "test").sync().isSuccess());
      assertTrue(client.disconnect().sync().isSuccess());
    }

    @Test
    @DisplayName("ping")
    void ping() throws InterruptedException {
      assertTrue(client.connect(true, 60, 60, "test").sync().isSuccess());
      assertTrue(client.ping().sync().isSuccess());
      assertTrue(client.disconnect().sync().isSuccess());
    }

    @Test
    @DisplayName("start pinger")
    void start_pinger() throws InterruptedException {
      assertTrue(client.connect(true, 2, 2, "test").sync().isSuccess());
      TimeUnit.SECONDS.sleep(4);
      assertTrue(client.channel().isActive());
    }

    @Test
    @DisplayName("stop pinger")
    void stop_pinger() throws InterruptedException {
      assertTrue(client.connect(true, 2, 0, "test").sync().isSuccess());
      TimeUnit.SECONDS.sleep(4);
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
  @Tag("subscription")
  class Subscription extends TestBase {

    @Test
    @DisplayName("subscribe")
    void subscribe() throws InterruptedException {
      Future<MqttQoS[]> future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertTrue(future.sync().isSuccess());

      MqttQoS[] results = future.getNow();
      assertNotNull(results);
      assertEquals(3, results.length);
      assertEquals(2, results[0].value());
      assertEquals(1, results[1].value());
      assertEquals(0, results[2].value());
    }

    @Test
    @DisplayName("subscribe failed")
    void subscribe_failed() {
      assertThatExceptionOfType(ClosedChannelException.class).isThrownBy(() -> {
        assertFalse(client.subscribe(
            MqttSubscription.qos2("test/#/2"),
            MqttSubscription.qos1("test/#/1"),
            MqttSubscription.qos0("test/#/0"))
            .sync().isSuccess());
      });
    }

    @Test
    @DisplayName("unsubscribe")
    void unsubscribe() throws InterruptedException {
      assertTrue(client.unsubscribe("test/2", "test/1", "test/0").sync().isSuccess());
    }

    @Test
    @DisplayName("unsubscribe failed")
    void unsubscribe_failed() {
      assertThatExceptionOfType(ClosedChannelException.class).isThrownBy(() -> {
        assertFalse(client.unsubscribe("test/#/2", "test/#/1", "test/#/0").sync().isSuccess());
      });
    }
  }

  @Nested
  @Tag("pubsub")
  class PubSub extends TestBase {

    @Test
    @DisplayName("pub0/sub0")
    void pub0_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/0/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/0", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/0/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub0/sub1")
    void pub0_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/0/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/1", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/0/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub0/sub2")
    void pub0_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/0/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/2", payload());
      assertTrue(future.sync().isSuccess());
      assertEquals(0, future.packetId());
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/0/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub1/sub0")
    void pub1_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub1/sub1")
    void pub1_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub1/sub2")
    void pub1_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub0")
    void pub2_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub2/sub1")
    void pub2_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub2")
    void pub2_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(2, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.received(msg.packetId()).sync().isSuccess());
      assertTrue(client.complete(msg.packetId()).sync().isSuccess());
    }
  }

  @Nested
  @Tag("retransmit")
  class Retransmit extends TestBase {

    @Test
    @DisplayName("pub1/sub0")
    void pub1_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub1/sub1")
    void pub1_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub1/sub2")
    void pub1_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/1/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub0")
    void pub2_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(0, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/0", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());
    }

    @Test
    @DisplayName("pub2/sub1")
    void pub2_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(1, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/1", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));

      assertTrue(client.ack(msg.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub2")
    void pub2_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertFalse(future.isDuplicate());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertThat(future.packetId()).isGreaterThan(0);
      assertTrue(future.isDuplicate());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertFalse(msg.isDuplicate());
      assertEquals(2, msg.qos().value());
      assertFalse(msg.isRetain());
      assertEquals("test/2/2", msg.topic());
      assertEquals(PAYLOAD, msg.payload().toString(StandardCharsets.UTF_8));
      assertTrue(msg.release());

      assertTrue(client.received(msg.packetId()).sync().isSuccess());
      assertTrue(client.complete(msg.packetId()).sync().isSuccess());
    }
  }
}
