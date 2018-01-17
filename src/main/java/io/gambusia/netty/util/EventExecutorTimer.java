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
package io.gambusia.netty.util;

import static io.gambusia.netty.util.Args.*;
import static io.netty.util.internal.logging.InternalLoggerFactory.*;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;

public class EventExecutorTimer implements Timer {

  private static final InternalLogger logger = getInstance(EventExecutorTimer.class);

  private final Map<Timeout, TimerTask> tasks = new ConcurrentHashMap<>();
  private final EventExecutorGroup executor;

  public EventExecutorTimer(EventExecutorGroup executor) {
    this.executor = checkNotNull(executor, "executor");
  }

  @Override
  public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    return new EventExecutorTimeout(this, task, delay, unit);
  }

  @Override
  public Set<Timeout> stop() {
    final Set<Timeout> unfinished = Collections.unmodifiableSet(tasks.keySet());
    unfinished.forEach(timeout -> timeout.cancel());
    return unfinished;
  }

  private static final class EventExecutorTimeout implements Timeout {

    private final EventExecutorTimer timer;
    private final TimerTask task;
    private final ScheduledFuture<?> future;

    EventExecutorTimeout(EventExecutorTimer timer, TimerTask task, long delay, TimeUnit unit) {
      this.timer = checkNotNull(timer, "timer");
      this.task = checkNotNull(task, "task");
      this.future = timer.executor.schedule(() -> {
        try {
          this.task.run(this);
        } catch (Throwable cause) {
          logger.warn("An exception was thrown by TimerTask.", cause);
        }
      }, delay, unit);

      this.future.addListener(f -> this.timer.tasks.remove(this));
      this.timer.tasks.put(this, this.task);
    }

    @Override
    public Timer timer() {
      return timer;
    }

    @Override
    public TimerTask task() {
      return task;
    }

    @Override
    public boolean isExpired() {
      return future.isDone();
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean cancel() {
      return future.cancel(false);
    }
  }
}
