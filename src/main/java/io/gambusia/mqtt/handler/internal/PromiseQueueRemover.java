
package io.gambusia.mqtt.handler.internal;

import java.util.Queue;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class PromiseQueueRemover<V, P extends Promise<?>> implements FutureListener<V> {

  private final Queue<P> promises;

  public PromiseQueueRemover(Queue<P> promises) {
    this.promises = promises;
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    if (!future.isSuccess()) {
      promises.poll();
    }
  }
}
