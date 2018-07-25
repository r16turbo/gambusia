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

import static io.gambusia.netty.util.Args.requireNonNull;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorTimer implements Timer {

  private static final InternalLogger logger;

  static {
    logger = InternalLoggerFactory.getInstance(ScheduledExecutorTimer.class);
  }

  private final IntObjectHashMap<Timeout> timeouts = new IntObjectHashMap<>();
  private final ScheduledExecutorService executor;

  private int timeoutId = 0;
  private boolean stopped = false;

  public ScheduledExecutorTimer(ScheduledExecutorService executor) {
    this.executor = requireNonNull(executor, "executor");
  }

  @Override
  public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    if (stopped) {
      throw new IllegalStateException("cannot be started once stopped");
    }
    return new ScheduledExecutorTimeout(this, task, delay, unit);
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

  private Future<?> schedule(int id, Runnable command, long delay, TimeUnit unit) {
    try {
      return executor.schedule(command, delay, unit);
    } catch (Exception e) {
      removeTimeout(id);
      throw e;
    }
  }

  private static final class ScheduledExecutorTimeout implements Timeout, Runnable {

    private final ScheduledExecutorTimer timer;
    private final TimerTask task;

    private final int id;
    private final Future<?> future;

    ScheduledExecutorTimeout(ScheduledExecutorTimer timer, TimerTask task,
        long delay, TimeUnit unit) {

      this.timer = requireNonNull(timer, "timer");
      this.task = requireNonNull(task, "task");
      requireNonNull(unit, "unit");
      this.id = timer.addTimeout(this);
      this.future = timer.schedule(id, this, delay, unit);
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
      if (future.cancel(false)) {
        timer.removeTimeout(id);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void run() {
      try {
        timer.removeTimeout(id);
        task.run(this);
      } catch (Throwable cause) {
        logger.warn("An exception was thrown by TimerTask.", cause);
      }
    }
  }
}
