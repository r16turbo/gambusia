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
import io.gambusia.mqtt.MqttPublishFuture;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;

public class MqttPublishPromise extends MqttTimeLimitPromise<Void> implements MqttPublishFuture {

  private final MqttArticle article;
  private final boolean duplicate;

  private int packetId;

  public MqttPublishPromise(EventExecutor executor, MqttArticle article) {
    this(executor, 0, null, article, 0);
  }

  public MqttPublishPromise(EventExecutor executor, long timeout, TimeUnit timeunit,
      MqttArticle article) {
    this(executor, timeout, timeunit, article, 0);
  }

  public MqttPublishPromise(EventExecutor executor, MqttPublishFuture future) {
    this(executor, 0, null,
        checkNotNull(future, "future").getArticle(), future.getPacketId());
  }

  public MqttPublishPromise(EventExecutor executor, long timeout, TimeUnit timeunit,
      MqttPublishFuture future) {
    this(executor, timeout, timeunit,
        checkNotNull(future, "future").getArticle(), future.getPacketId());
  }

  protected MqttPublishPromise(EventExecutor executor, long timeout, TimeUnit timeunit,
      MqttArticle article, int packetId) {
    super(executor, timeout, timeunit);
    this.article = checkNotNull(article, "article");
    this.duplicate = packetId > 0;
    this.packetId = packetId;
  }

  @Override
  public MqttArticle getArticle() {
    return article;
  }

  @Override
  public int getPacketId() {
    return packetId;
  }

  public void setPacketId(int packetId) {
    this.packetId = packetId;
  }

  @Override
  public boolean isDuplicate() {
    return duplicate;
  }

  @Override
  public boolean isReleasePending() {
    return (!isDone() || isSuccess()) && article.getQoS() == MqttQoS.EXACTLY_ONCE;
  }

  @Override
  public void run(Timeout timeout) {
    switch (article.getQoS()) {
      case AT_LEAST_ONCE:
        tryFailure(new TimeoutException("PUBACK"));
        break;
      case EXACTLY_ONCE:
        tryFailure(new TimeoutException("PUBREC"));
        break;
      default:
        tryFailure(new TimeoutException("unknown"));
        break;
    }
  }
}
