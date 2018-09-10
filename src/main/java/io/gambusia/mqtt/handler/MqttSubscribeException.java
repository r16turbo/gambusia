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

package io.gambusia.mqtt.handler;

import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttSubscribeException extends IllegalStateException {

  private static final long serialVersionUID = -5095305821092199572L;

  private final MqttQoS[] returnCodes;

  public MqttSubscribeException(String message, MqttQoS... returnCodes) {
    super(message);
    this.returnCodes = returnCodes;
  }

  public int returnCodeLength() {
    return returnCodes.length;
  }

  public MqttQoS returnCode(int index) {
    return returnCodes[index];
  }
}
