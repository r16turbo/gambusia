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
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorTimer implements Timer {

  private static final InternalLogger logger;

  static {
    logger = InternalLoggerFactory.getInstance(ScheduledExecutorTimer.class);
  }

  private final Map<Timeout, Void> timeouts = new WeakHashMap<>();
  private final ScheduledExecutorService executor;

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
      unfinished = new HashSet<>(timeouts.keySet());
    }
    for (Timeout timeout : unfinished) {
      timeout.cancel();
    }
    return unfinished;
  }

  private ScheduledFuture<Void> register(TimeoutRunner timeout, long delay, TimeUnit unit) {
    synchronized (timeouts) {
      if (timeouts.containsKey(timeout)) {
        throw new RejectedExecutionException("timeout has already been bound");
      }
      ScheduledFuture<Void> future = executor.schedule(timeout, delay, unit);
      timeouts.put(timeout, null);
      return future;
    }
  }

  private void unregister(TimeoutRunner timeout) {
    synchronized (timeouts) {
      timeouts.remove(timeout);
    }
  }

  private interface TimeoutRunner extends Timeout, Callable<Void> {
  }

  private static final class ScheduledExecutorTimeout implements TimeoutRunner {

    private final ScheduledExecutorTimer timer;
    private final TimerTask task;
    private final ScheduledFuture<Void> future;

    ScheduledExecutorTimeout(ScheduledExecutorTimer timer, TimerTask task,
        long delay, TimeUnit unit) {

      this.timer = requireNonNull(timer, "timer");
      this.task = requireNonNull(task, "task");
      this.future = timer.register(this, delay, requireNonNull(unit, "unit"));
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
      // expired is not a done of future
      return isCancelled() ? false : future.getDelay(TimeUnit.NANOSECONDS) <= 0;
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean cancel() {
      if (future.cancel(false)) {
        timer.unregister(this);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Void call() throws Exception {
      try {
        timer.unregister(this);
        task.run(this);
      } catch (Throwable cause) {
        logger.warn("An exception was thrown by TimerTask.", cause);
      }
      return null;
    }
  }
}
