# Gambusia/MQTT Java Client

The Gambusia/MQTT is an Netty-based MQTT v3.1.1 client library that run on the JVM.


## Concepts

The Gambusia/MQTT aims to implement minimal MQTT client functionality based on Netty.
We only implement the mandatory specifications of MQTT that doesn't exist in Netty.
So we don't implement features such as topic filters and persistence.
Also, the implementor decides an ambiguous implementation (e.g. retransmission) in the protocol sequence.

## Using the Gambusia/MQTT

### Declaring a dependency for Gradle

It hosts the Maven repository using the GitHub Pages.
Please use the Maven Central repository are other dependencies.

```gradle
repositories {
    maven { url 'https://r16turbo.github.io/maven/' }
}
dependencies {
    compile 'io.gambusia:gambusia:0.9.1'
}
```

## Getting Started

The included code below is a very basic sample same behavior to [Eclipse Paho Java Client's Getting Started](https://github.com/eclipse/paho.mqtt.java/blob/master/README.md#getting-started).

```java
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import io.gambusia.mqtt.MqttAsyncClient;
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

public class MqttPublishSample {

  public static void main(String[] args) {

    String topic    = "MQTT Examples";
    String content  = "Message from MqttPublishSample";
    MqttQoS qos     = MqttQoS.EXACTLY_ONCE;
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
      Channel ch = b.connect(broker.getHost(), broker.getPort()).sync().channel();
      MqttAsyncClient sampleClient = new MqttAsyncClient(ch, 1, TimeUnit.SECONDS);
      System.out.println("Connecting to broker: " + broker);
      sampleClient.connect(false, 60, clientId).sync();
      System.out.println("Connected");
      System.out.println("Publishing message: " + content);
      ChannelPromise promise = ch.newPromise();
      ByteBuf payload = Unpooled.wrappedBuffer(content.getBytes(StandardCharsets.UTF_8));
      sampleClient.publish(qos, false, topic, payload).addListener((MqttPublishFuture publish) -> {
        if (publish.isSuccess()) {
          if (publish.isReleasePending()) {
            sampleClient.release(publish.packetId()).addListener(release -> {
              if (release.isSuccess()) {
                promise.setSuccess();
              } else {
                promise.setFailure(release.cause());
              }
            });
          } else {
            promise.setSuccess();
          }
        } else {
          promise.setFailure(publish.cause());
        }
      });
      promise.sync();
      System.out.println("Message published");
      sampleClient.disconnect();
      ch.closeFuture().sync();
      System.out.println("Disconnected");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      workerGroup.shutdownGracefully();
    }
  }
}
```
