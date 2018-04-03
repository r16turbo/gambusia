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

public class MqttConnectionRefusedException extends Exception {

  private static final long serialVersionUID = -2698691263461588284L;

  private final byte returnCode;

  public MqttConnectionRefusedException(byte returnCode) {
    super(toMessage(returnCode));
    this.returnCode = returnCode;
  }

  public byte returnCode() {
    return returnCode;
  }

  private static String toMessage(byte returnCode) {
    switch (returnCode) {
      case 0:
        throw new IllegalArgumentException("Connection Accepted");
      case 1:
        return "Unacceptable protocol version";
      case 2:
        return "Identifier rejected";
      case 3:
        return "Server unavailable";
      case 4:
        return "Bad user name or password";
      case 5:
        return "Not authorized";
      default:
        throw new IllegalArgumentException("Reserved for future use");
    }
  }
}
