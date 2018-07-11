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

import static io.gambusia.netty.util.Args.checkNotEmpty;
import static io.gambusia.netty.util.Args.checkNotNull;

import io.gambusia.mqtt.MqttArticle;
import io.gambusia.mqtt.MqttConnectResult;
import io.gambusia.mqtt.handler.MqttPinger;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MqttConnectPromise extends MqttPromise<MqttConnectResult> {

  private final String protocolName;
  private final int protocolLevel;
  private final boolean cleanSession;
  private final int keepAlive;
  private final MqttPinger pinger;
  private final String clientId;
  private final MqttArticle will;
  private final String username;
  private final byte[] password;

  public MqttConnectPromise(EventExecutor executor, long timeout, TimeUnit unit,
      String protocolName, int protocolLevel,
      boolean cleanSession, int keepAlive, MqttPinger pinger,
      String clientId, MqttArticle will, String username, byte[] password) {

    super(executor, timeout, unit);
    this.protocolName = checkNotEmpty(protocolName, "protocolName");
    this.protocolLevel = protocolLevel;
    this.cleanSession = cleanSession;
    this.keepAlive = keepAlive;
    this.pinger = checkNotNull(pinger, "pinger");
    this.clientId = clientId;
    this.will = will;
    this.username = username;
    this.password = password;
  }

  public String protocolName() {
    return protocolName;
  }

  public int protocolLevel() {
    return protocolLevel;
  }

  public boolean isCleanSession() {
    return cleanSession;
  }

  public int keepAlive() {
    return keepAlive;
  }

  public MqttPinger pinger() {
    return pinger;
  }

  public String clientId() {
    return clientId;
  }

  public MqttArticle will() {
    return will;
  }

  public String username() {
    return username;
  }

  public byte[] password() {
    return password;
  }

  @Override
  public void run(Timeout timeout) {
    tryFailure(new TimeoutException("CONNACK"));
  }
}
