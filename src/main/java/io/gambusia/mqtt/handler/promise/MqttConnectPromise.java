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
package io.gambusia.mqtt.handler.promise;

import static io.gambusia.netty.util.Args.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.gambusia.mqtt.MqttArticle;
import io.gambusia.mqtt.MqttConnectResult;
import io.gambusia.mqtt.handler.MqttPinger;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;

public class MqttConnectPromise extends MqttTimeLimitPromise<MqttConnectResult> {

  private final String protocolName;
  private final int protocolLevel;
  private final boolean cleanSession;
  private final int keepAlive;
  private final MqttPinger pinger;
  private final String clientId;
  private final MqttArticle will;
  private final String username;
  private final byte[] password;

  public MqttConnectPromise(EventExecutor executor,
      String protocolName, int protocolLevel, boolean cleanSession,
      int keepAlive, MqttPinger pinger,
      String clientId, MqttArticle will, String username, byte[] password) {

    this(executor, 0, null, protocolName, protocolLevel, cleanSession, keepAlive, pinger,
        clientId, will, username, password);
  }

  public MqttConnectPromise(EventExecutor executor, long timeout, TimeUnit timeunit,
      String protocolName, int protocolLevel,
      boolean cleanSession, int keepAlive, MqttPinger pinger,
      String clientId, MqttArticle will, String username, byte[] password) {

    super(executor, timeout, timeunit);
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
