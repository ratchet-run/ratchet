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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.SchedulerUtils;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Polls for due recurring masters and delegates to {@link RecurringJobExecutor} to spawn children.
 * Uses a singleton lease so only one node processes recurring jobs at a time.
 */
@ApplicationScoped
public class DefaultRecurringScheduler implements RecurringScheduler {

  private static final Logger log = Logger.getLogger(DefaultRecurringScheduler.class);
  private static final String LEASE_NAME = "recurringScheduler";
  private static final Duration LEASE_TTL = Duration.ofMinutes(5);

  private final AtomicBoolean started = new AtomicBoolean();
  private final Object scheduleLock = new Object();
  private final ExecutorProvider executorProvider;
  private final RecurringJobStore recurringJobStore;
  private final SingletonLeaseService singletonLeaseService;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final RecurringJobExecutor recurringJobExecutor;
  private final PollerScheduler pollerScheduler;
  private final Clock clock;

  private volatile Config config = new Config(1000, 60000, 20);
  private volatile long currentDelayMs;

  @SuppressWarnings("java:S3077")
  private volatile Future<?> handle;

  @SuppressWarnings("java:S3077")
  private volatile ScheduledExecutorService executor;

  protected DefaultRecurringScheduler() {
    this.executorProvider = null;
    this.recurringJobStore = null;
    this.singletonLeaseService = null;
    this.nodeIdentityProvider = null;
    this.recurringJobExecutor = null;
    this.pollerScheduler = null;
    this.clock = null;
  }

  @Inject
  public DefaultRecurringScheduler(
      ExecutorProvider executorProvider,
      RecurringJobStore recurringJobStore,
      SingletonLeaseService singletonLeaseService,
      NodeIdentityProvider nodeIdentityProvider,
      RecurringJobExecutor recurringJobExecutor,
      PollerScheduler pollerScheduler,
      Clock clock) {
    this.executorProvider = executorProvider;
    this.recurringJobStore = recurringJobStore;
    this.singletonLeaseService = singletonLeaseService;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.recurringJobExecutor = recurringJobExecutor;
    this.pollerScheduler = pollerScheduler;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public long getCurrentDelayMs() {
    return currentDelayMs;
  }

  @Override
  public void configure(long minPollMs, long maxPollMs, int batchLimit) {
    this.config = new Config(minPollMs, maxPollMs, batchLimit);
  }

  @Override
  public void init() {
    if (!started.compareAndSet(false, true)) {
      log.warn("RecurringScheduler already initialized; skipping re-run");
      return;
    }

    executor = executorProvider.getScheduledExecutor();
    Config snapshot = config;
    currentDelayMs = snapshot.minPollMs();
    scheduleNext(snapshot.minPollMs());
    log.infof(
        "RecurringScheduler started (minPoll=%s ms, maxPoll=%s ms)",
        snapshot.minPollMs(), snapshot.maxPollMs());
  }

  /** Forces an immediate poll cycle. */
  @Override
  public void kick() {
    if (!started.get()) {
      return;
    }
    synchronized (scheduleLock) {
      if (!started.get()) {
        return;
      }
      Future<?> current = handle;
      if (current != null) {
        current.cancel(false);
        handle = null;
      }
      scheduleNextLocked(config.minPollMs());
    }
    log.debug("RecurringScheduler kicked — immediate scan scheduled");
  }

  @Override
  public void stop() {
    started.set(false);
    synchronized (scheduleLock) {
      if (handle != null) {
        handle.cancel(false);
      }
      handle = null;
    }
  }

  void run() {
    if (!started.get()) {
      return;
    }

    ScheduledFuture<?> renewalTask = null;
    Optional<SingletonLease> lease = Optional.empty();
    try {
      lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
      if (lease.isEmpty()) {
        scheduleNext(config.minPollMs());
        return;
      }

      SingletonLease acquiredLease = lease.get();
      AtomicBoolean leaseValid = new AtomicBoolean(true);
      renewalTask =
          executor.scheduleWithFixedDelay(
              () -> renewLease(acquiredLease, leaseValid), 2, 2, TimeUnit.MINUTES);

      int processedCount =
          recurringJobExecutor.process(config.batchLimit(), nodeIdentityProvider.getNodeId());

      if (!leaseValid.get()) {
        return;
      }

      if (processedCount > 0) {
        pollerScheduler.wakeup();
      }

      long nextDelay = calculateNextDelay(processedCount);
      scheduleNext(nextDelay);
    } catch (Exception ex) {
      if (!started.get()) {
        return;
      }
      // CDI context gone (e.g. Arquillian undeploy) — stop permanently, next deploy starts fresh
      if (SchedulerUtils.isCdiContextGone(ex)) {
        started.set(false);
        log.info("RecurringScheduler detected inactive CDI context — stopping permanently");
        return;
      }
      log.error("RecurringScheduler failed", ex);
      try {
        scheduleNext(config.minPollMs());
      } catch (Exception e) {
        log.debug("Cannot reschedule recurring scan — scheduler will restart on next deploy", e);
      }
    } catch (Error error) {
      log.error("RecurringScheduler failed with unrecoverable error", error);
      throw error;
    } finally {
      if (renewalTask != null && !renewalTask.isCancelled()) {
        renewalTask.cancel(false);
      }
      lease.ifPresent(SingletonLease::close);
    }
  }

  private long calculateNextDelay(int processedCount) {
    Config snapshot = config;
    if (processedCount > 0) {
      return snapshot.minPollMs();
    }

    Optional<Instant> earliestNextFire = recurringJobStore.findEarliestRecurringNextFire();

    if (earliestNextFire.isEmpty()) {
      return snapshot.maxPollMs();
    }

    long msUntilNextFire =
        Duration.between(Instant.now(effective()), earliestNextFire.get()).toMillis();

    long targetDelay = Math.max(msUntilNextFire - 500, snapshot.minPollMs());
    return Math.min(targetDelay, snapshot.maxPollMs());
  }

  private Clock effective() {
    if (clock == null) {
      throw new IllegalStateException("RecurringScheduler clock was not initialized");
    }
    return clock;
  }

  private void renewLease(SingletonLease lease, AtomicBoolean leaseValid) {
    // Don't close the lease here — the run() finally block holds the sole close site to avoid
    // double-unlock on stores that don't ownership-verify (which could release a peer node's
    // lock between our first close and the redundant second one).
    try {
      if (!lease.renew(LEASE_TTL)) {
        log.warnf("RecurringScheduler could not renew singleton lease %s", lease.name());
        leaseValid.set(false);
        stop();
      }
    } catch (Exception e) {
      log.warnf(e, "RecurringScheduler lease renewal failed for %s", lease.name());
      leaseValid.set(false);
      stop();
    }
  }

  private void scheduleNext(long delayMs) {
    synchronized (scheduleLock) {
      scheduleNextLocked(delayMs);
    }
  }

  private void scheduleNextLocked(long delayMs) {
    if (!started.get()) {
      return;
    }
    currentDelayMs = delayMs;
    handle = executor.schedule(this::run, delayMs, TimeUnit.MILLISECONDS);
  }

  private record Config(long minPollMs, long maxPollMs, int batchLimit) {}
}
