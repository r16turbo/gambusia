# Gambusia/MQTT Java Client
[![Build Status](https://travis-ci.com/r16turbo/gambusia.svg?branch=master)](https://travis-ci.com/r16turbo/gambusia)

The Gambusia/MQTT is an Netty-based MQTT v3.1.1 Client Library that run on the JVM.


## Concepts

The Gambusia/MQTT aims to implement Minimal MQTT Client Library based on Netty.
It implements the mandatory specification of MQTT which does not exist in Netty.
Therefore, don't plan to implement features such as automatic reconnection and persistent.
Ambiguous sequences (e.g. retransmission) should be implemented by developers.

## Using the Gambusia/MQTT

### Declaring a dependency for Gradle

It hosts the Maven repository using the GitHub Pages.
Please use the Maven Central repository are other dependencies.

```gradle
repositories {
    maven { url 'https://r16turbo.github.io/maven/' }
}
dependencies {
    compile 'io.gambusia:gambusia:X.X.X'
}
```
 - See the page for current release: https://github.com/r16turbo/gambusia/releases

## Getting Started

The included code below is a very basic sample same behavior to [Eclipse Paho Java Client's Getting Started](https://github.com/eclipse/paho.mqtt.java/blob/master/README.md#getting-started).

```java
import io.gambusia.mqtt.MqttAsyncClient;
import io.gambusia.mqtt.MqttPublishFuture;
import io.gambusia.mqtt.MqttSubscriber;
import io.gambusia.mqtt.handler.MqttClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class MqttPublishSample {

  public static void main(String[] args) {

    String topic    = "MQTT Examples";
    String content  = "Message from MqttPublishSample";
    MqttQoS qos     = MqttQoS.valueOf(2);
    URI broker      = URI.create("tcp://iot.eclipse.org:1883");
    String clientId = "JavaSample";

    EventLoopGroup workerGroup = new NioEventLoopGroup();
    MqttSubscriber subscriber = (ch, msg) -> msg.release();

    Bootstrap b = new Bootstrap();
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
        p.addLast("mqttHandler", new MqttClientHandler(subscriber));
        p.addLast("loggingHandler", new LoggingHandler());
      }
    });

    try {
      Channel channel = b.connect(broker.getHost(), broker.getPort()).sync().channel();
      MqttAsyncClient client = new MqttAsyncClient(channel, 1, TimeUnit.SECONDS);
      System.out.println("Connecting to broker: " + broker);
      client.connect(true, 60, 60, clientId).sync();
      System.out.println("Connected");
      System.out.println("Publishing message: " + content);
      ByteBuf payload = Unpooled.wrappedBuffer(content.getBytes(StandardCharsets.UTF_8));
      MqttPublishFuture publish = client.publish(qos, false, topic, payload);
      if (publish.sync().isSuccess()) {
        System.out.println("Message published");
        if (publish.isReleasePending()) {
          Future<Void> release = client.release(publish.packetId());
          if (release.sync().isSuccess()) {
            System.out.println("Message released");
          }
        }
      } else {
        publish.article().release();
      }
      client.disconnect();
      channel.closeFuture().sync();
      System.out.println("Disconnected");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      workerGroup.shutdownGracefully();
    }
  }
}
```
