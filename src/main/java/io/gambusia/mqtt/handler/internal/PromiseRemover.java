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

package io.gambusia.mqtt.handler.internal;

import io.gambusia.mqtt.handler.internal.Hash.ImmutableHash;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.ConcurrentMap;

public class PromiseRemover<V, P extends Promise<?>> implements FutureListener<V> {

  private final ConcurrentMap<Hash, P> promises;
  private final Hash hash;
  private final P promise;

  public PromiseRemover(ConcurrentMap<Hash, P> promises, int packetId, P promise) {
    this.promises = promises;
    this.hash = new ImmutableHash(packetId);
    this.promise = promise;
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    if (!future.isSuccess()) {
      promises.remove(hash, promise);
    }
  }
}
