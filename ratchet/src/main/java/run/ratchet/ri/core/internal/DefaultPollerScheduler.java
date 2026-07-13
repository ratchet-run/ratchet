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
package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.SchedulerUtils;
import run.ratchet.spi.ExecutorProvider;

/**
 * Owns the scheduling infrastructure for the job poller: executor lifecycle, dynamic poll delays,
 * and wakeup signals. Delegates each poll cycle to {@link Poller#tick()}.
 *
 * @see Poller
 */
@ApplicationScoped
public class DefaultPollerScheduler implements PollerScheduler {

  private static final Logger log = Logger.getLogger(DefaultPollerScheduler.class);

  private final AtomicBoolean started = new AtomicBoolean();
  private final Object scheduleLock = new Object();
  private final ExecutorProvider executorProvider;
  private final PollerCycleExecutor pollerCycleExecutor;

  @SuppressWarnings("java:S3077")
  private volatile Future<?> handle;

  /**
   * Cached executor reference resolved once during {@link #start()}, avoiding CDI proxy lookups.
   */
  @SuppressWarnings("java:S3077")
  private volatile ScheduledExecutorService executor;

  private boolean cycleRunning;
  private boolean wakeupPending;

  protected DefaultPollerScheduler() {
    this.executorProvider = null;
    this.pollerCycleExecutor = null;
  }

  @Inject
  public DefaultPollerScheduler(
      ExecutorProvider executorProvider, PollerCycleExecutor pollerCycleExecutor) {
    this.executorProvider = executorProvider;
    this.pollerCycleExecutor = pollerCycleExecutor;
  }

  @Override
  public void start() {
    ScheduledExecutorService resolvedExecutor = executorProvider.getScheduledExecutor();
    if (!started.compareAndSet(false, true)) {
      log.warn("DefaultPollerScheduler already started; skipping re-start");
      return;
    }

    executor = resolvedExecutor;
    log.info("DefaultPollerScheduler starting");
    synchronized (scheduleLock) {
      scheduleNextLocked(0);
    }
    log.info("DefaultPollerScheduler started");
  }

  @Override
  public void stop() {
    if (!started.compareAndSet(true, false)) {
      return;
    }

    log.info("DefaultPollerScheduler stopping");
    synchronized (scheduleLock) {
      wakeupPending = false;
      cancelCurrentScheduleLocked();
    }
    log.info("DefaultPollerScheduler stopped");
  }

  /**
   * Wakes up the poller to immediately check for available jobs.
   *
   * <p>Called when a job notification is received from the cluster, indicating that new work is
   * available.
   */
  @Override
  public void wakeup() {
    if (!started.get()) {
      return;
    }

    pollerCycleExecutor.onWakeup();

    synchronized (scheduleLock) {
      if (!started.get()) {
        return;
      }
      if (cycleRunning) {
        wakeupPending = true;
        log.debug("DefaultPollerScheduler wakeup coalesced into running poll cycle");
        return;
      }
      cancelCurrentScheduleLocked();
      scheduleNextLocked(0);
    }

    log.debug("DefaultPollerScheduler wakeup triggered - immediate poll scheduled");
  }

  void scheduleNext(long delayMs) {
    synchronized (scheduleLock) {
      scheduleNextLocked(delayMs);
    }
  }

  private void scheduleNextLocked(long delayMs) {
    if (!started.get()) {
      return;
    }
    handle = executor.schedule(this::executePollCycle, delayMs, TimeUnit.MILLISECONDS);
  }

  private void cancelCurrentScheduleLocked() {
    Future<?> currentHandle = handle;
    if (currentHandle != null && !currentHandle.isDone()) {
      currentHandle.cancel(false);
    }
    handle = null;
  }

  @SuppressWarnings("java:S1181")
  private void executePollCycle() {
    synchronized (scheduleLock) {
      if (!started.get()) {
        return;
      }
      if (cycleRunning) {
        wakeupPending = true;
        return;
      }
      cycleRunning = true;
      handle = null;
    }

    long nextDelayMs = 5000;
    try {
      nextDelayMs = pollerCycleExecutor.tick();
    } catch (Throwable t) {
      if (!started.get()) {
        finishPollCycle(0, false);
        return;
      }
      // CDI context gone (e.g. Arquillian undeploy) — stop permanently, next deploy starts fresh
      if (SchedulerUtils.isCdiContextGone(t)) {
        synchronized (scheduleLock) {
          started.set(false);
          cycleRunning = false;
          wakeupPending = false;
          handle = null;
        }
        log.info("Poll cycle detected inactive CDI context — stopping permanently");
        return;
      }
      log.error("Poll cycle failed", t);
    }

    finishPollCycle(nextDelayMs, true);
  }

  private void finishPollCycle(long nextDelayMs, boolean reschedule) {
    synchronized (scheduleLock) {
      cycleRunning = false;
      if (!reschedule || !started.get()) {
        return;
      }
      long delayMs = wakeupPending ? 0 : nextDelayMs;
      wakeupPending = false;
      try {
        scheduleNextLocked(delayMs);
      } catch (Exception e) {
        log.warn("Cannot reschedule poll cycle; scheduler will restart on next deploy", e);
      }
    }
  }
}
