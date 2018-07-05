package io.gambusia.mqtt.handler;

import static io.gambusia.netty.util.Args.*;
import java.util.concurrent.TimeUnit;
import io.gambusia.mqtt.handler.promise.MqttPingPromise;
import io.netty.util.concurrent.EventExecutor;

public class MqttPinger {

  private final long timeout;
  private final TimeUnit unit;

  public MqttPinger(long timeout, TimeUnit unit) {
    this.timeout = checkPositive(timeout, "timeout");
    this.unit = checkNotNull(unit, "unit");
  }

  protected MqttPingPromise ping(EventExecutor executor) {
    return new MqttPingPromise(executor, timeout, unit);
  }
}
