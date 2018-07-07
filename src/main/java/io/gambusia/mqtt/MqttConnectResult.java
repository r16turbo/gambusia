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

package io.gambusia.mqtt;

public class MqttConnectResult {

  private final boolean sessionPresent;
  private final short returnCode;

  public MqttConnectResult(boolean sessionPresent, short returnCode) {
    this.sessionPresent = sessionPresent;
    this.returnCode = returnCode;
  }

  public boolean isSessionPresent() {
    return sessionPresent;
  }

  public short returnCode() {
    return returnCode;
  }
}
