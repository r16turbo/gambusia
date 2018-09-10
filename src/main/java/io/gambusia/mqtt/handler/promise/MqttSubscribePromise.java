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

package io.gambusia.mqtt.handler.promise;

import static io.gambusia.netty.util.Args.requireNonContainsNull;

import io.gambusia.mqtt.MqttSubscribeFuture;
import io.gambusia.mqtt.MqttSubscription;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MqttSubscribePromise extends MqttPromise<MqttQoS[]> implements MqttSubscribeFuture {

  private final List<MqttSubscription> subscriptions;

  private int successes = -1;
  private int downgrades = -1;

  public MqttSubscribePromise(EventExecutor executor, long timeout, TimeUnit unit,
      MqttSubscription... subscriptions) {
    super(executor, timeout, unit);
    this.subscriptions = Collections.unmodifiableList(
        Arrays.asList(requireNonContainsNull(subscriptions, "subscriptions")));
  }

  @Override
  public final MqttMessageType messageType() {
    return MqttMessageType.SUBSCRIBE;
  }

  @Override
  public List<MqttSubscription> subscriptions() {
    return subscriptions;
  }

  @Override
  public boolean isAllSuccess() {
    if (successes < 0 && isDone()) {
      successes = downgrades = 0;
      if (isSuccess()) {
        final List<MqttSubscription> requests = subscriptions();
        final MqttQoS[] results = getNow();
        final int size = Math.min(requests.size(), results.length);
        for (int index = 0; index < size; index++) {
          MqttQoS granted = results[index];
          if (granted == MqttQoS.FAILURE) {
            continue;
          }
          MqttQoS request = requests.get(index).qos();
          if (granted.compareTo(request) < 0) {
            downgrades++;
          }
          successes++;
        }
      }
    }
    return successes == subscriptions().size();
  }

  @Override
  public boolean isCompleteSuccess() {
    return isAllSuccess() && downgrades == 0;
  }

  @Override
  public void run(Timeout timeout) {
    tryFailure(new TimeoutException("No response message: expected=SUBACK"));
  }
}
