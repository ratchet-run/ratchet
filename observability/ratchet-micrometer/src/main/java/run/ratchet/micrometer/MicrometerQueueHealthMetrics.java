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
package run.ratchet.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToDoubleFunction;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.QueueHealthSnapshot;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Exports the queue-health snapshot as Micrometer gauges.
 *
 * <p>Each refresh runs several aggregate store queries, so snapshots are cached for 15 seconds per
 * node. Values are zero when the store lacks the {@code JobAnalyticsStore} capability. Applications
 * can disable these gauges with a Micrometer {@code MeterFilter.deny} on the {@code ratchet.queue}
 * prefix.
 */
@ApplicationScoped
public class MicrometerQueueHealthMetrics implements SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(MicrometerQueueHealthMetrics.class);
  private static final Duration SNAPSHOT_TTL = Duration.ofSeconds(15);

  private final MeterRegistry registry;
  private final JobQueryService jobQueryService;
  private final Clock clock;
  private final List<Meter> meters = new ArrayList<>();
  private final Object snapshotLock = new Object();

  private QueueHealthSnapshot cachedSnapshot;
  private Instant lastFetchTime;

  // Required by the CDI proxy; business methods on this instance are no-ops.
  MicrometerQueueHealthMetrics() {
    this(null, null, null);
  }

  @Inject
  public MicrometerQueueHealthMetrics(
      MeterRegistry registry, Instance<JobQueryService> jobQueryService) {
    this(registry, jobQueryService.isResolvable() ? jobQueryService.get() : null);
  }

  public MicrometerQueueHealthMetrics(MeterRegistry registry, JobQueryService jobQueryService) {
    this(registry, jobQueryService, Clock.systemUTC());
  }

  MicrometerQueueHealthMetrics(
      MeterRegistry registry, JobQueryService jobQueryService, Clock clock) {
    this.registry = registry;
    this.jobQueryService = jobQueryService;
    this.clock = clock;
  }

  @Override
  public void afterStart() {
    if (registry == null || jobQueryService == null) {
      return;
    }
    synchronized (meters) {
      if (!meters.isEmpty()) {
        return;
      }
      for (JobStatus status : JobStatus.values()) {
        register(
            gauge(
                    "ratchet.queue.jobs",
                    "Current jobs by status",
                    snapshot ->
                        switch (status) {
                          case PENDING -> snapshot.pendingCount();
                          case RUNNING -> snapshot.runningCount();
                          case WAITING -> snapshot.waitingCount();
                          case PAUSED -> snapshot.pausedCount();
                          case FAILED -> snapshot.failedCount();
                          case SUCCEEDED -> snapshot.succeededCount();
                          case CANCELED -> snapshot.canceledCount();
                        })
                .tag("status", status.name()));
      }
      register(
          gauge(
              "ratchet.queue.ready", "Pending jobs ready to run", QueueHealthSnapshot::readyCount));
      register(gauge("ratchet.queue.stuck", "Stuck running jobs", QueueHealthSnapshot::stuckCount));
      for (JobType type : JobType.values()) {
        register(
            gauge(
                    "ratchet.queue.pending.type",
                    "Pending jobs by type",
                    snapshot -> snapshot.pendingByType().getOrDefault(type, 0L))
                .tag("type", type.name()));
      }
      for (JobPriority priority : JobPriority.values()) {
        register(
            gauge(
                    "ratchet.queue.pending.priority",
                    "Pending jobs by priority",
                    snapshot -> snapshot.pendingByPriority().getOrDefault(priority, 0L))
                .tag("priority", priority.name()));
      }
      register(
          gauge(
                  "ratchet.queue.wait.p95",
                  "95th percentile queue wait time",
                  QueueHealthSnapshot::p95QueueWaitMs)
              .baseUnit("milliseconds"));
      register(
          gauge(
                  "ratchet.queue.oldest.pending.age",
                  "Age of the oldest pending job",
                  this::oldestPendingAge)
              .baseUnit("seconds"));
    }
  }

  @Override
  public void beforeStop() {
    if (registry == null || jobQueryService == null) {
      return;
    }
    synchronized (meters) {
      for (Meter meter : meters) {
        registry.remove(meter);
      }
      meters.clear();
    }
  }

  private Gauge.Builder<MicrometerQueueHealthMetrics> gauge(
      String name, String description, ToDoubleFunction<QueueHealthSnapshot> value) {
    return Gauge.builder(name, this, metrics -> metrics.value(value)).description(description);
  }

  private void register(Gauge.Builder<MicrometerQueueHealthMetrics> builder) {
    meters.add(builder.strongReference(true).register(registry));
  }

  private double value(ToDoubleFunction<QueueHealthSnapshot> value) {
    QueueHealthSnapshot snapshot = snapshot();
    return snapshot == null ? Double.NaN : value.applyAsDouble(snapshot);
  }

  private QueueHealthSnapshot snapshot() {
    if (registry == null || jobQueryService == null) {
      return null;
    }
    synchronized (snapshotLock) {
      if (lastFetchTime == null || !clock.instant().isBefore(lastFetchTime.plus(SNAPSHOT_TTL))) {
        try {
          cachedSnapshot = jobQueryService.getQueueHealth();
        } catch (RuntimeException e) {
          log.warnf(e, "Could not refresh queue health metrics: %s", e.getMessage());
        } finally {
          lastFetchTime = clock.instant();
        }
      }
      return cachedSnapshot;
    }
  }

  private double oldestPendingAge(QueueHealthSnapshot snapshot) {
    Instant oldest = snapshot.oldestPendingJobTime();
    return oldest == null
        ? 0
        : Math.max(0, Duration.between(oldest, clock.instant()).toMillis() / 1000.0);
  }
}
