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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.gambusia.mqtt.handler.MqttClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
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

class MqttAsyncClientTest {

  private static final Logger logger = LogManager.getLogger(MqttAsyncClientTest.class);

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  static final String HOST = "127.0.0.1";
  static final int PORT = 1883;

  static final byte[] PAYLOAD = "Hello World!".getBytes(StandardCharsets.UTF_8);

  static ByteBuf payload() {
    return ByteBufAllocator.DEFAULT.directBuffer(PAYLOAD.length).writeBytes(PAYLOAD);
  }

  static BlockingQueue<MqttPublication> queue;
  static EventLoopGroup workerGroup;
  static Bootstrap b;

  private final MqttAsyncClient client = new MqttAsyncClient(10, TimeUnit.SECONDS);

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    queue = new ArrayBlockingQueue<>(8);
    workerGroup = new NioEventLoopGroup();
    b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.option(ChannelOption.TCP_NODELAY, true);
    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast("mqttDecoder", new MqttDecoder());
        p.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        p.addLast("mqttHandler", new MqttClientHandler((channel, msg) -> queue.put(msg)));
        p.addLast("loggingHandler", new LoggingHandler());
      }
    });
  }

  @AfterAll
  static void tearDownAfterClass() throws Exception {
    workerGroup.shutdownGracefully().sync();
  }

  @BeforeEach
  void setUp() throws Exception {
    ChannelFuture f = b.connect(HOST, PORT);
    client.set(f.sync().channel());
  }

  @AfterEach
  void tearDown() throws Exception {
    client.channel().close();
    client.channel().closeFuture().sync();
  }

  @Test
  @DisplayName("connect")
  void test01_connect() throws InterruptedException {
    Future<MqttConnectResult> future = client.connect(true, 2, "test");
    assertTrue(future.sync().isSuccess());

    MqttConnectResult result = future.getNow();
    assertNotNull(result);
    assertEquals(0, result.returnCode());
    assertFalse(result.isSessionPresent());
  }

  @Test
  @DisplayName("disconnect")
  void test02_disconnect() throws InterruptedException {
    assertTrue(client.disconnect().sync().isSuccess());
  }

  @Nested
  @Tag("keepalive")
  @DisplayName("KeepAlive")
  class Test01_KeepAlive {

    @BeforeEach
    void setUp() throws Exception {
      client.set(b.connect(HOST, PORT).sync().channel());
      client.connect(true, 2, "test").sync();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.disconnect();
      client.channel().closeFuture().sync();
    }

    @Test
    @DisplayName("ping -> pong")
    void test01_ping_pong() throws InterruptedException {
      assertTrue(client.ping().sync().isSuccess());
    }

    @Test
    @DisplayName("wait for keepAlive + 1 sec")
    void test02_wait_for_keepalive_plus_1sec() throws InterruptedException {
      TimeUnit.SECONDS.sleep(3);
      assertTrue(client.ping().sync().isSuccess());
    }
  }

  @Nested
  @Tag("subscription")
  @DisplayName("Subscription")
  class Test02_Subscription {

    @BeforeEach
    void setUp() throws Exception {
      client.set(b.connect(HOST, PORT).sync().channel());
      client.connect(true, 60, "test").sync();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.disconnect();
      client.channel().closeFuture().sync();
    }

    @Test
    @DisplayName("subscribe")
    void test01_subscribe() throws InterruptedException {
      Future<MqttQoS[]> future = client.subscribe(
          MqttSubscription.qos2("test/+/2"),
          MqttSubscription.qos1("test/+/1"),
          MqttSubscription.qos0("test/+/0"));
      assertTrue(future.sync().isSuccess());

      MqttQoS[] results = future.getNow();
      assertNotNull(results);
      assertEquals(3, results.length);
      assertEquals(MqttQoS.valueOf(2), results[0]);
      assertEquals(MqttQoS.valueOf(1), results[1]);
      assertEquals(MqttQoS.valueOf(0), results[2]);
    }

    @Test
    @DisplayName("subscribe failed")
    void test02_subscribe_failed() throws InterruptedException {
      assertFalse(client.subscribe(
          MqttSubscription.qos2("test/#/2"),
          MqttSubscription.qos1("test/#/1"),
          MqttSubscription.qos0("test/#/0"))
          .await().isSuccess());
    }

    @Test
    @DisplayName("unsubscribe")
    void test03_unsubscribe() throws InterruptedException {
      assertTrue(client.unsubscribe("test/2", "test/1", "test/0").sync().isSuccess());
    }

    @Test
    @DisplayName("unsubscribe failed")
    void test04_unsubscribe_failed() throws InterruptedException {
      assertFalse(client.unsubscribe("test/#/2", "test/#/1", "test/#/0").await().isSuccess());
    }
  }

  @Nested
  @Tag("pubsub")
  @DisplayName("PubSub")
  class Test03_PubSub {

    @BeforeEach
    void setUp() throws Exception {
      client.set(b.connect(HOST, PORT).sync().channel());
      client.connect(true, 60, "test").sync();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.disconnect();
      client.channel().closeFuture().sync();
    }

    @Test
    @DisplayName("pub0/sub0")
    void test01_pub0_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/0/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/0", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/0/0", msg.topic());
    }

    @Test
    @DisplayName("pub0/sub1")
    void test02_pub0_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/0/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/1", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/0/1", msg.topic());
    }

    @Test
    @DisplayName("pub0/sub2")
    void test03_pub0_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/0/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish0(false, "test/0/2", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/0/2", msg.topic());
    }

    @Test
    @DisplayName("pub1/sub0")
    void test04_pub1_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/1/0", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub1/sub1")
    void test05_pub1_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/1", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub1/sub2")
    void test06_pub1_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/2", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub0")
    void test07_pub2_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/2/0", msg.topic());
    }

    @Test
    @DisplayName("pub2/sub1")
    void test08_pub2_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/2/1", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub2")
    void test09_pub2_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(2), msg.qos());
      assertEquals("test/2/2", msg.topic());
      assertTrue(client.received(future.packetId()).sync().isSuccess());
      assertTrue(client.complete(future.packetId()).sync().isSuccess());
    }
  }

  @Nested
  @Tag("retransmit")
  @DisplayName("Retransmit")
  class Test04_Retransmit {

    @BeforeEach
    void setUp() throws Exception {
      client.set(b.connect(HOST, PORT).sync().channel());
      client.connect(true, 60, "test").sync();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.disconnect();
      client.channel().closeFuture().sync();
    }

    @Test
    @DisplayName("pub1/sub0")
    void test01_pub1_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/1/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/1/0", msg.topic());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/1/0", msg.topic());
    }

    @Test
    @DisplayName("pub1/sub1")
    void test02_pub1_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/1/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/1", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/1", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub1/sub2")
    void test03_pub1_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/1/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish1(false, "test/1/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      MqttPublication msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/2", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertFalse(future.isReleasePending());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/1/2", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub0")
    void test04_pub2_sub0() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos0("test/2/0")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/0", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(0), msg.qos());
      assertEquals("test/2/0", msg.topic());
    }

    @Test
    @DisplayName("pub2/sub1")
    void test05_pub2_sub1() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos1("test/2/1")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/1", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(1), msg.qos());
      assertEquals("test/2/1", msg.topic());
      assertTrue(client.ack(future.packetId()).sync().isSuccess());
    }

    @Test
    @DisplayName("pub2/sub2")
    void test06_pub2_sub2() throws InterruptedException {
      assertTrue(client.subscribe(MqttSubscription.qos2("test/2/2")).sync().isSuccess());

      MqttPublishFuture future = client.publish2(false, "test/2/2", payload().retain());
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());

      MqttPublication msg = queue.poll();
      assertNull(msg);

      future = client.publish(future);
      assertTrue(future.sync().isSuccess());
      assertTrue(future.isReleasePending());
      assertTrue(client.release(future.packetId()).sync().isSuccess());

      msg = queue.poll(1, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(MqttQoS.valueOf(2), msg.qos());
      assertEquals("test/2/2", msg.topic());
      assertTrue(client.received(future.packetId()).sync().isSuccess());
      assertTrue(client.complete(future.packetId()).sync().isSuccess());
    }
  }

  @Nested
  @DisplayName("finish")
  class ZZZ {

    @Test
    @DisplayName("finish")
    void test99_finish() throws InterruptedException {
      MqttPublication publication;
      while ((publication = queue.poll()) != null) {
        try (MqttPublication msg = publication) {
          logger.warn("unread: dup={}, qos={}, retain={}, packetId={}, topic={}, payload={}",
              msg.isDuplicate(), msg.qos(), msg.isRetain(),
              msg.packetId(), msg.topic(),
              msg.payload().toString(StandardCharsets.UTF_8));
        }
      }
    }
  }
}
