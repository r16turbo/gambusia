package io.gambusia.mqtt.handler;

import java.util.concurrent.TimeUnit;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.netty.util.concurrent.EventExecutor;

public class MqttPinger {

  private final long timeout;
  private final TimeUnit unit;

  public MqttPinger() {
    this(0, null);
  }

  public MqttPinger(long timeout, TimeUnit unit) {
    this.timeout = timeout;
    this.unit = unit;
  }

  protected MqttPingPromise ping(EventExecutor executor) {
    return new MqttPingPromise(executor, timeout, unit);
  }
}
