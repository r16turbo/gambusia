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

package io.gambusia.mqtt;

import static io.gambusia.netty.util.Args.checkNotEmpty;

import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttSubscription {

  private final MqttQoS qos;
  private final String topicFilter;

  public MqttSubscription(MqttQoS qos, String topicFilter) {
    this.qos = qos;
    this.topicFilter = checkNotEmpty(topicFilter, "topicFilter");
  }

  public MqttQoS qos() {
    return qos;
  }

  public String topicFilter() {
    return topicFilter;
  }

  public static MqttSubscription qos0(String topicFilter) {
    return new MqttSubscription(MqttQoS.AT_MOST_ONCE, topicFilter);
  }

  public static MqttSubscription qos1(String topicFilter) {
    return new MqttSubscription(MqttQoS.AT_LEAST_ONCE, topicFilter);
  }

  public static MqttSubscription qos2(String topicFilter) {
    return new MqttSubscription(MqttQoS.EXACTLY_ONCE, topicFilter);
  }
}
