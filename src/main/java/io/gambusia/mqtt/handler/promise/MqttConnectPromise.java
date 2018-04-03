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
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;

public class MqttConnectPromise extends MqttTimeLimitPromise<MqttConnectResult> {

  private final String protocolName;
  private final int protocolLevel;
  private final boolean cleanSession;
  private final int keepAlive;
  private final String clientId;
  private final MqttArticle will;
  private final String username;
  private final byte[] password;

  private final long pingInterval;
  private final TimeUnit pingTimeunit;

  public MqttConnectPromise(EventExecutor executor,
      String protocolName, int protocolLevel, boolean cleanSession, int keepAlive,
      String clientId, MqttArticle will, String username, byte[] password,
      long pingInterval, TimeUnit pingTimeunit) {

    this(executor, 0, null, protocolName, protocolLevel, cleanSession, keepAlive,
        clientId, will, username, password, pingInterval, pingTimeunit);
  }

  public MqttConnectPromise(EventExecutor executor, long timeout, TimeUnit timeunit,
      String protocolName, int protocolLevel,
      boolean cleanSession, int keepAlive, String clientId, MqttArticle will,
      String username, byte[] password,
      long pingInterval, TimeUnit pingTimeunit) {

    super(executor, timeout, timeunit);
    this.protocolName = checkNotEmpty(protocolName, "protocolName");
    this.protocolLevel = protocolLevel;
    this.cleanSession = cleanSession;
    this.keepAlive = keepAlive;
    this.clientId = clientId;
    this.will = will;
    this.username = username;
    this.password = password;

    this.pingInterval = pingInterval;
    this.pingTimeunit = pingTimeunit;
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

  public long pingInterval() {
    return pingInterval;
  }

  public TimeUnit pingTimeunit() {
    return pingTimeunit;
  }

  @Override
  public void run(Timeout timeout) {
    tryFailure(new TimeoutException("CONNACK"));
  }
}
