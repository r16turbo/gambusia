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

package io.gambusia.mqtt.handler.internal;

import io.netty.util.Timeout;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

public class TimeoutCanceller<V> implements FutureListener<V> {

  private final Timeout timeout;

  public TimeoutCanceller(Timeout timeout) {
    this.timeout = timeout;
  }

  @Override
  public void operationComplete(Future<V> future) throws Exception {
    timeout.cancel();
  }
}
