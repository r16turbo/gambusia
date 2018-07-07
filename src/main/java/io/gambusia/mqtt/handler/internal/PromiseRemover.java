
package io.gambusia.mqtt.handler.internal;

import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class PromiseRemover<V, P extends Promise<?>> implements FutureListener<V> {

  private final IntObjectMap<P> promises;
  private final int packetId;
  private final P promise;

  public PromiseRemover(IntObjectMap<P> promises, int packetId, P promise) {
    this.promises = promises;
    this.packetId = packetId;
    this.promise = promise;
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    if (!future.isSuccess() && this.promise == promises.get(packetId)) {
      promises.remove(packetId);
    }
  }
}
