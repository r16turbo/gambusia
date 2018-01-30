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
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import io.gambusia.mqtt.handler.MqttClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
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
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

class MqttAsyncClientTest {

  private static final Logger logger = LogManager.getLogger(MqttAsyncClientTest.class);

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  static final String HOST = "127.0.0.1";
  static final int PORT = 1883;

  static final long TIMEOUT = 1;
  static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;

  static final String TOPIC0 = "test/0";
  static final String TOPIC1 = "test/1";
  static final String TOPIC2 = "test/2";

  static EventLoopGroup workerGroup;
  static Bootstrap b;

  static MqttAsyncClient client;
  static BlockingQueue<MqttPublication> subscribeQueue;

  private final AtomicInteger count = new AtomicInteger();
  private final MqttSubscription[] subscriptions = new MqttSubscription[] {
      MqttSubscription.qos2(TOPIC0),
      MqttSubscription.qos2(TOPIC1),
      MqttSubscription.qos2(TOPIC2),
  };
  private final String[] topicFilters = Arrays.stream(subscriptions)
      .map(s -> s.getTopicFilter()).toArray(String[]::new);
  private final ByteBuf payload = Unpooled.unreleasableBuffer(
      Unpooled.wrappedBuffer("Hello World!".getBytes(StandardCharsets.UTF_8)));

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    workerGroup = new NioEventLoopGroup();
    subscribeQueue = new ArrayBlockingQueue<>(8);
    MqttSubscriber subscriber = (ch, msg) -> subscribeQueue.put(msg);

    b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.option(ChannelOption.TCP_NODELAY, true);
    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast("mqttDecoder", new MqttDecoder());
        p.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        p.addLast("mqttHandler", new MqttClientHandler(subscriber, TIMEOUT, TIMEUNIT));
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
    count.set(0);
  }

  @AfterEach
  void tearDown() throws Exception {}

  @Test
  @DisplayName("tcp connect")
  void test000() throws InterruptedException {
    client = new MqttAsyncClient(b.connect(HOST, PORT).sync().channel());
  }

  @Test
  @DisplayName("mqtt connect")
  void test001() throws InterruptedException, ExecutionException {
    MqttConnectResult result = client.connect(true, 2, 1, TimeUnit.SECONDS, "test").get();
    assertNotNull(result);
    assertEquals(0, result.getReturnCode());
    assertFalse(result.isSessionPresent());
  }

  @Test
  @DisplayName("subscribe")
  void test010() throws InterruptedException, ExecutionException {
    MqttQoS[] results = client.subscribe(subscriptions).get();
    assertNotNull(results);
    assertEquals(subscriptions.length, results.length);

    if (logger.isInfoEnabled()) {
      for (int i = 0; i < subscriptions.length; i++) {
        final MqttSubscription subscription = subscriptions[i];
        final MqttQoS grantedQoS = results[i];
        logger.info("subscribe: topicFilter={}, requestedQoS={}, grantedQoS={}",
            subscription.getTopicFilter(), subscription.getQoS(), grantedQoS);
      }
    }
  }

  @Test
  @DisplayName("ping -> pong")
  void test011() throws InterruptedException {
    client.ping().addListener(f -> assertTrue(f.isSuccess())).sync();
  }

  @Test
  @DisplayName("ping -> failed")
  void test012() {
    assertThrows(TimeoutException.class, () -> {
      client.ping(1, TimeUnit.NANOSECONDS).sync();
    });
  }

  @ParameterizedTest(name = "keepAlive timeout={0}ms")
  @ValueSource(ints = {1000, 2000})
  void test013(int timeout) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(timeout);
  }

  @Test
  @DisplayName("publish qos0")
  void test020() throws InterruptedException, ExecutionException {
    MqttPublishFuture future = client.publish0(false, TOPIC0, payload);
    assertNull(future.get());
    if (future.isReleasePending()) {
      client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
    }
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));
  }

  @Test
  @DisplayName("publish qos1")
  void test021() throws InterruptedException, ExecutionException {
    MqttPublishFuture future = client.publish1(false, TOPIC1, payload);
    assertNull(future.get());
    if (future.isReleasePending()) {
      client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
    }
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));
  }

  @Test
  @DisplayName("publish qos2")
  void test022() throws InterruptedException, ExecutionException {
    MqttPublishFuture future = client.publish2(false, TOPIC2, payload);
    assertNull(future.get());
    if (future.isReleasePending()) {
      client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
    }
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));
  }

  @Test
  @DisplayName("publish qos1 retry")
  void test031() throws InterruptedException, ExecutionException {
    MqttPublishFuture failure = client.publish1(false, TOPIC1, payload, 1, TimeUnit.NANOSECONDS);
    assertThrows(TimeoutException.class, () -> failure.sync());
    // duplicate
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));

    MqttPublishFuture future = client.publish(failure);
    assertNull(future.get());
    assertEquals(failure.getPacketId(), future.getPacketId());
    if (future.isReleasePending()) {
      client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
    }
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));
  }

  @Test
  @DisplayName("publish qos2 retry")
  void test032() throws InterruptedException, ExecutionException {
    MqttPublishFuture failure = client.publish2(false, TOPIC2, payload, 1, TimeUnit.NANOSECONDS);
    assertThrows(TimeoutException.class, () -> failure.sync());

    MqttPublishFuture future = client.publish(failure);
    assertNull(future.get());
    assertEquals(failure.getPacketId(), future.getPacketId());
    if (future.isReleasePending()) {
      client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
    }
    assertTimeout(Duration.ofSeconds(1), () -> received(subscribeQueue.take()));
  }

  @Test
  @DisplayName("unsubscribe")
  void test080() throws InterruptedException {
    client.unsubscribe(topicFilters).addListener(f -> assertTrue(f.isSuccess())).sync();
  }

  @ParameterizedTest(name = "publish timeout={0}ms, qos={1}")
  @CsvSource({
      "1000, AT_MOST_ONCE",
      "1000, AT_LEAST_ONCE",
      "1000, EXACTLY_ONCE",
  })
  void test090(int timeoutMillis, MqttQoS qos) throws InterruptedException, ExecutionException {
    final long deadline = System.currentTimeMillis() + timeoutMillis;
    while (deadline > System.currentTimeMillis()) {
      final String topic = "test/" + count.incrementAndGet();
      MqttPublishFuture future = client.publish(qos, false, topic, payload);
      assertNull(future.get());
      if (future.isReleasePending()) {
        client.release(future.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
      }
    }
    logger.info("publish: {}tps", () -> {
      final int count = this.count.get();
      return timeoutMillis < 1000L ? count : count / (timeoutMillis / 1000L);
    });
  }

  @Test
  @DisplayName("disconnect")
  void test099() throws InterruptedException {
    client.disconnect().addListener(f -> assertTrue(f.isSuccess())).sync();

    client.channel().closeFuture().sync();
  }

  @Test
  @DisplayName("finish")
  void test999() throws InterruptedException {
    MqttPublication publication;
    while ((publication = subscribeQueue.poll()) != null) {
      try (MqttPublication msg = publication) {
        logger.warn("unread: dup={}, qos={}, retain={}, packetId={}, topic={}, payload={}",
            msg.isDuplicate(), msg.getQoS(), msg.isRetain(),
            msg.getPacketId(), msg.getTopic(),
            msg.getPayload().toString(StandardCharsets.UTF_8));
      }
    }
  }

  private void received(MqttPublication publication) throws InterruptedException {
    try (MqttPublication msg = publication) {
      logger.info("receive: dup={}, qos={}, retain={}, packetId={}, topic={}, payload={}",
          msg.isDuplicate(), msg.getQoS(), msg.isRetain(),
          msg.getPacketId(), msg.getTopic(),
          msg.getPayload().toString(StandardCharsets.UTF_8));

      switch (msg.getQoS()) {
        case AT_LEAST_ONCE:
          client.ack(msg.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
          break;
        case EXACTLY_ONCE:
          client.received(msg.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
          client.complete(msg.getPacketId()).addListener(f -> assertTrue(f.isSuccess())).sync();
          break;
        default:
      }
    }
  }
}
