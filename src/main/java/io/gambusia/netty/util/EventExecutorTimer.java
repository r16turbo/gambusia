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

package io.gambusia.netty.util;

import static io.gambusia.netty.util.Args.checkNotNull;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class EventExecutorTimer implements Timer {

  private static final InternalLogger logger;

  static {
    logger = InternalLoggerFactory.getInstance(EventExecutorTimer.class);
  }

  private final IntObjectHashMap<Timeout> timeouts = new IntObjectHashMap<>();
  private final EventExecutorGroup executor;

  private int timeoutId = 0;
  private boolean stopped = false;

  public EventExecutorTimer(EventExecutorGroup executor) {
    this.executor = checkNotNull(executor, "executor");
  }

  @Override
  public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    if (stopped) {
      throw new IllegalStateException("cannot be started once stopped");
    }
    return new EventExecutorTimeout(this, task, delay, unit);
  }

  @Override
  public Set<Timeout> stop() {
    this.stopped = true;
    final Set<Timeout> unfinished;
    synchronized (timeouts) {
      unfinished = new HashSet<>(timeouts.values());
    }
    for (Timeout timeout : unfinished) {
      timeout.cancel();
    }
    return unfinished;
  }

  private int addTimeout(Timeout timeout) {
    synchronized (timeouts) {
      final int id = timeoutId++;
      timeouts.put(id, timeout);
      return id;
    }
  }

  private void removeTimeout(int id) {
    synchronized (timeouts) {
      timeouts.remove(id);
    }
  }

  private static final class EventExecutorTimeout implements Timeout {

    private final EventExecutorTimer timer;
    private final TimerTask task;

    private final int id;
    private final Future<?> future;

    EventExecutorTimeout(EventExecutorTimer timer, TimerTask task, long delay, TimeUnit unit) {
      this.timer = checkNotNull(timer, "timer");
      this.task = checkNotNull(task, "task");
      this.id = timer.addTimeout(this);
      this.future = timer.executor
          .schedule(new TimeoutSchedule(this), delay, unit)
          .addListener(new TimeoutRemover<>(timer, id));
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

    private static class TimeoutSchedule implements Runnable {

      private final Timeout timeout;

      TimeoutSchedule(Timeout timeout) {
        this.timeout = timeout;
      }

      @Override
      public void run() {
        try {
          timeout.task().run(timeout);
        } catch (Throwable cause) {
          logger.warn("An exception was thrown by TimerTask.", cause);
        }
      }
    }

    private static class TimeoutRemover<V> implements FutureListener<V> {

      private final EventExecutorTimer timer;
      private final int id;

      TimeoutRemover(EventExecutorTimer timer, int id) {
        this.timer = timer;
        this.id = id;
      }

      @Override
      public void operationComplete(Future<V> future) {
        timer.removeTimeout(id);
      }
    }
  }
}
