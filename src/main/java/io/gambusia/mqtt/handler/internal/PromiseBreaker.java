package io.gambusia.mqtt.handler.internal;

import java.util.function.Consumer;
import io.netty.util.concurrent.Promise;

public class PromiseBreaker implements Consumer<Promise<?>> {

  private final Throwable cause;

  public PromiseBreaker(Throwable cause) {
    this.cause = cause;
  }

  @Override
  public void accept(Promise<?> promise) {
    if (promise != null) {
      promise.tryFailure(cause);
    }
  }
}
