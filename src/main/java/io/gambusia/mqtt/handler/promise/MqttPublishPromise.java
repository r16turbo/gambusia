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
    this(executor, article, 0, 0, null);
  }

  public MqttPublishPromise(EventExecutor executor, MqttArticle article,
      long timeout, TimeUnit timeunit) {
    this(executor, article, 0, timeout, timeunit);
  }

  public MqttPublishPromise(EventExecutor executor, MqttArticle article, int packetId) {
    this(executor, article, packetId, 0, null);
  }

  public MqttPublishPromise(EventExecutor executor, MqttArticle article, int packetId,
      long timeout, TimeUnit timeunit) {
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
