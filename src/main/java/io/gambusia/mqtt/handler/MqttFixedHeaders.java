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

package io.gambusia.mqtt.handler;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

public interface MqttFixedHeaders {

  MqttFixedHeader CONNECT_HEADER =
      new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, -1);

  MqttFixedHeader CONNACK_HEADER =
      new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 2);

  MqttFixedHeader PUBACK_HEADER =
      new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);

  MqttFixedHeader PUBREC_HEADER =
      new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);

  MqttFixedHeader PUBREL_HEADER =
      new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2);

  MqttFixedHeader PUBCOMP_HEADER =
      new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);

  MqttFixedHeader SUBSCRIBE_HEADER =
      new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, -1);

  MqttFixedHeader SUBACK_HEADER =
      new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, -1);

  MqttFixedHeader UNSUBSCRIBE_HEADER =
      new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, -1);

  MqttFixedHeader UNSUBACK_HEADER =
      new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);

  MqttFixedHeader PINGREQ_HEADER =
      new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);

  MqttFixedHeader PINGRESP_HEADER =
      new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);

  MqttFixedHeader DISCONNECT_HEADER =
      new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
}
