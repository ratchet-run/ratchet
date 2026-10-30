/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.consumer.fixtures;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import run.ratchet.spi.ExecutorProvider;

/** Store-only contracts boot the real integration without racing its background timers. */
@Factory
@Requires(property = "consumer.store-contract", value = "true")
public class StoreContractExecutors {
  @Singleton
  @Bean(preDestroy = "close")
  DormantExecutors executors() {
    return new DormantExecutors();
  }

  public static final class DormantExecutors implements ExecutorProvider, AutoCloseable {
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final ScheduledExecutorService timers = new DormantTimers();

    public ExecutorService getJobExecutor() {
      return workers;
    }

    public ScheduledExecutorService getScheduledExecutor() {
      return timers;
    }

    public void close() {
      workers.shutdownNow();
      timers.shutdownNow();
    }
  }

  private static final class DormantTimers extends ScheduledThreadPoolExecutor {
    DormantTimers() {
      super(1);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return new DormantFuture<>();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
      return new DormantFuture<>();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      return new DormantFuture<>();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return new DormantFuture<>();
    }
  }

  private static final class DormantFuture<V> implements ScheduledFuture<V> {
    private boolean cancelled;

    public long getDelay(TimeUnit unit) {
      return unit.convert(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public int compareTo(Delayed other) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }

    public synchronized boolean cancel(boolean interrupt) {
      cancelled = true;
      notifyAll();
      return true;
    }

    public synchronized boolean isCancelled() {
      return cancelled;
    }

    public synchronized boolean isDone() {
      return cancelled;
    }

    public synchronized V get() throws InterruptedException {
      while (!cancelled) wait();
      throw new CancellationException();
    }

    public synchronized V get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      if (!cancelled) unit.timedWait(this, timeout);
      if (cancelled) throw new CancellationException();
      throw new TimeoutException("Dormant store-contract timer");
    }
  }
}
