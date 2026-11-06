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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.api.JobType;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventType;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.EventOutboxStore;
import run.ratchet.store.spi.JobStore;

/** Singleton recovery of terminal events left behind by a crashed publisher. */
@ApplicationScoped
public class TerminalEventRelay {
  private static final Logger log = Logger.getLogger(TerminalEventRelay.class);
  private final EventOutboxStore store;
  private final Clock clock;
  private final BatchStore batchStore;
  private final SingletonLeaseService leases;
  private final InternalEventPublisher publisher;
  private final RatchetOptions options;
  private final Object runLock = new Object();
  private volatile boolean stopped;
  private volatile ScheduledFuture<?> handle;

  protected TerminalEventRelay() {
    store = null;
    clock = null;
    batchStore = null;
    leases = null;
    publisher = null;
    options = null;
  }

  @Inject
  public TerminalEventRelay(
      Instance<EventOutboxStore> store,
      Instance<BatchStore> batches,
      SingletonLeaseService leases,
      InternalEventPublisher publisher,
      RatchetOptions options,
      Clock clock) {
    this(
        store.isResolvable() ? store.get() : null,
        batches.isResolvable() ? batches.get() : null,
        leases,
        publisher,
        options,
        clock);
  }

  public TerminalEventRelay(
      JobStore store,
      SingletonLeaseService leases,
      InternalEventPublisher publisher,
      RatchetOptions options,
      Clock clock) {
    this(
        store.capability(EventOutboxStore.class).orElse(null),
        store.capability(BatchStore.class).orElse(null),
        leases,
        publisher,
        options,
        clock);
  }

  public TerminalEventRelay(
      EventOutboxStore store,
      BatchStore batchStore,
      SingletonLeaseService leases,
      InternalEventPublisher publisher,
      RatchetOptions options,
      Clock clock) {
    this.store = store;
    this.clock = clock;
    this.batchStore = batchStore;
    this.leases = leases;
    this.publisher = publisher;
    this.options = options;
  }

  public synchronized void start(ScheduledExecutorService executor) {
    long interval = options.node().orphanScanIntervalSeconds();
    handle =
        executor.scheduleWithFixedDelay(
            this::relayNow, Math.min(interval, 3), interval, TimeUnit.SECONDS);
  }

  public void stop() {
    stopped = true;
    ScheduledFuture<?> current = handle;
    if (current != null) current.cancel(false);
    synchronized (runLock) {
      // Wait for the current bounded batch before its store can be torn down.
    }
  }

  /** Runs one bounded recovery pass, also used by deterministic integration tests. */
  public void relayNow() {
    synchronized (runLock) {
      relayWithLock();
    }
  }

  private void relayWithLock() {
    if (stopped) return;
    int delivered = 0;
    try {
      var lease =
          leases.tryAcquire(
              "terminalEventRelay",
              Duration.ofSeconds(options.node().orphanRecoveryLeaseTtlSeconds()));
      if (lease.isEmpty()) return;
      try (SingletonLease ignored = lease.get()) {
        Instant cutoff =
            (clock != null ? clock : Clock.systemUTC())
                .instant()
                .minusSeconds(options.node().orphanGraceSeconds());
        for (int batch = 0; batch < 10 && !stopped; batch++) {
          List<OutboxEvent> rows = store.findEventsCreatedBefore(cutoff, 100);
          if (rows.isEmpty()) break;
          for (OutboxEvent row : rows) {
            Long duration = row.executionTimeMs();
            if (row.eventType() == OutboxEventType.JOB_COMPLETED
                && row.jobType() == JobType.BATCH
                && duration == null
                && batchStore != null) {
              duration =
                  batchStore
                      .findBatchMetrics(row.jobId())
                      .map(BatchMetricsEntity::getTotalDurationMs)
                      .orElse(null);
            }
            AbstractJobSchedulerEvent event;
            try {
              event = TerminalEventMapper.toEvent(row, duration);
            } catch (RuntimeException invalid) {
              // Rows are read oldest first, so a row that can never become an event would block
              // every later row. Drop it with the reason instead.
              log.errorf(
                  invalid,
                  "Dropping outbox event %s for job %s: it cannot be rebuilt as an event",
                  row.eventId(),
                  row.jobId());
              continue;
            }
            publisher.publish(event);
            delivered++;
          }
          store.deleteEvents(rows.stream().map(OutboxEvent::eventId).toList());
        }
      }
    } catch (RuntimeException failure) {
      log.warn("Terminal event relay failed", failure);
    } finally {
      if (delivered > 0) log.infof("Redelivered %s terminal event(s)", delivered);
    }
  }
}
