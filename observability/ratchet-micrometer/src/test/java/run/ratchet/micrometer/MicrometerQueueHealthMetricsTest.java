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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.QueueHealthSnapshot;

class MicrometerQueueHealthMetricsTest {

  private static final Instant NOW = Instant.parse("2026-09-28T12:00:00Z");
  private static final int GAUGE_COUNT = 11 + JobType.values().length + JobPriority.values().length;

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final JobQueryService jobQueryService = mock(JobQueryService.class);
  private final MutableClock clock = new MutableClock(NOW);
  private final MicrometerQueueHealthMetrics metrics =
      new MicrometerQueueHealthMetrics(registry, jobQueryService, clock);

  @AfterEach
  void closeRegistry() {
    registry.close();
  }

  @Test
  void registersAllGaugesOnStartWithSnapshotValues() {
    when(jobQueryService.getQueueHealth()).thenReturn(snapshot(14, 9, 1200, NOW.minusSeconds(90)));

    assertTrue(registry.getMeters().isEmpty());
    metrics.afterStart();

    assertEquals(GAUGE_COUNT, registry.getMeters().size());
    verifyNoInteractions(jobQueryService);
    Map<JobStatus, Double> counts =
        Map.of(
            JobStatus.PENDING, 14.0,
            JobStatus.RUNNING, 2.0,
            JobStatus.WAITING, 7.0,
            JobStatus.PAUSED, 6.0,
            JobStatus.FAILED, 3.0,
            JobStatus.SUCCEEDED, 4.0,
            JobStatus.CANCELED, 5.0);
    counts.forEach(
        (status, count) ->
            assertEquals(
                count.doubleValue(), gauge("ratchet.queue.jobs", "status", status.name()).value()));
    assertEquals(9.0, gauge("ratchet.queue.ready").value());
    assertEquals(8.0, gauge("ratchet.queue.stuck").value());
    for (JobType type : JobType.values()) {
      double expected = type == JobType.SINGLE ? 10 : type == JobType.BATCH ? 4 : 0;
      assertEquals(expected, gauge("ratchet.queue.pending.type", "type", type.name()).value());
    }
    for (JobPriority priority : JobPriority.values()) {
      double expected = priority == JobPriority.NORMAL ? 9 : priority == JobPriority.HIGH ? 5 : 0;
      assertEquals(
          expected, gauge("ratchet.queue.pending.priority", "priority", priority.name()).value());
    }
    assertEquals(1200.0, gauge("ratchet.queue.wait.p95").value());
    assertEquals("milliseconds", gauge("ratchet.queue.wait.p95").getId().getBaseUnit());
    assertEquals(90.0, gauge("ratchet.queue.oldest.pending.age").value());
    assertEquals("seconds", gauge("ratchet.queue.oldest.pending.age").getId().getBaseUnit());
    for (Meter meter : registry.getMeters()) {
      assertEquals(Meter.Type.GAUGE, meter.getId().getType());
      assertFalse(meter.getId().getDescription().isBlank());
    }
    verify(jobQueryService).getQueueHealth();
  }

  @Test
  void sharesSnapshotAcrossReadsUntilTtlExpires() {
    when(jobQueryService.getQueueHealth())
        .thenReturn(
            snapshot(14, 9, 1200, NOW.minusSeconds(90)),
            snapshot(30, 25, 1800, NOW.minusSeconds(60)));
    metrics.afterStart();

    readAllGauges();
    readAllGauges();
    clock.advance(Duration.ofSeconds(14));
    readAllGauges();
    assertEquals(14.0, gauge("ratchet.queue.jobs", "status", "PENDING").value());
    assertEquals(9.0, gauge("ratchet.queue.ready").value());
    verify(jobQueryService).getQueueHealth();

    clock.advance(Duration.ofSeconds(2));
    readAllGauges();
    assertEquals(30.0, gauge("ratchet.queue.jobs", "status", "PENDING").value());
    assertEquals(25.0, gauge("ratchet.queue.ready").value());
    assertEquals(1800.0, gauge("ratchet.queue.wait.p95").value());
    assertEquals(76.0, gauge("ratchet.queue.oldest.pending.age").value());
    verify(jobQueryService, times(2)).getQueueHealth();
  }

  @Test
  void keepsGoodSnapshotOnFailureAndWaitsAnotherTtlBeforeRetrying() {
    when(jobQueryService.getQueueHealth())
        .thenReturn(snapshot(14, 9, 1200, NOW.minusSeconds(90)))
        .thenThrow(new IllegalStateException("Store unavailable"))
        .thenReturn(snapshot(30, 25, 1800, NOW.minusSeconds(60)));
    metrics.afterStart();
    readAllGauges();

    clock.advance(Duration.ofSeconds(15));
    readAllGauges();
    assertEquals(14.0, gauge("ratchet.queue.jobs", "status", "PENDING").value());
    assertEquals(9.0, gauge("ratchet.queue.ready").value());
    assertEquals(1200.0, gauge("ratchet.queue.wait.p95").value());
    assertEquals(105.0, gauge("ratchet.queue.oldest.pending.age").value());
    verify(jobQueryService, times(2)).getQueueHealth();

    clock.advance(Duration.ofSeconds(14));
    readAllGauges();
    verify(jobQueryService, times(2)).getQueueHealth();

    clock.advance(Duration.ofSeconds(1));
    assertEquals(25.0, gauge("ratchet.queue.ready").value());
    readAllGauges();
    verify(jobQueryService, times(3)).getQueueHealth();
  }

  @Test
  void reportsNanUntilFirstSuccessfulFetchAndThrottlesInitialFailure() {
    when(jobQueryService.getQueueHealth())
        .thenThrow(new IllegalStateException("Store unavailable"))
        .thenReturn(snapshot(14, 9, 1200, NOW.minusSeconds(90)));
    metrics.afterStart();

    for (Meter meter : registry.getMeters()) {
      assertTrue(Double.isNaN(((Gauge) meter).value()), meter.getId().toString());
    }
    clock.advance(Duration.ofSeconds(14));
    for (Meter meter : registry.getMeters()) {
      assertTrue(Double.isNaN(((Gauge) meter).value()), meter.getId().toString());
    }
    verify(jobQueryService).getQueueHealth();

    clock.advance(Duration.ofSeconds(1));
    assertEquals(9.0, gauge("ratchet.queue.ready").value());
    for (Meter meter : registry.getMeters()) {
      assertTrue(Double.isFinite(((Gauge) meter).value()), meter.getId().toString());
    }
    verify(jobQueryService, times(2)).getQueueHealth();
  }

  @Test
  void nullQueryServiceRegistersNothing() {
    MicrometerQueueHealthMetrics unavailable =
        new MicrometerQueueHealthMetrics(registry, (JobQueryService) null);

    assertDoesNotThrow(unavailable::afterStart);
    assertDoesNotThrow(unavailable::beforeStop);
    assertTrue(registry.getMeters().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void unresolvableQueryServiceRegistersNothing() {
    Instance<JobQueryService> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);
    MicrometerQueueHealthMetrics unavailable = new MicrometerQueueHealthMetrics(registry, instance);

    assertDoesNotThrow(unavailable::afterStart);
    assertDoesNotThrow(unavailable::beforeStop);
    assertTrue(registry.getMeters().isEmpty());
    verify(instance, never()).get();
  }

  @Test
  @SuppressWarnings("unchecked")
  void resolvableQueryServicePublishesGauges() {
    Instance<JobQueryService> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(jobQueryService);
    when(jobQueryService.getQueueHealth()).thenReturn(snapshot(14, 9, 1200, null));
    MicrometerQueueHealthMetrics injectable = new MicrometerQueueHealthMetrics(registry, instance);

    injectable.afterStart();

    assertEquals(GAUGE_COUNT, registry.getMeters().size());
    assertEquals(9.0, gauge("ratchet.queue.ready").value());
    verify(instance).get();
    verify(jobQueryService).getQueueHealth();
  }

  @Test
  void nullRegistryDoesNotQueryStore() {
    MicrometerQueueHealthMetrics unavailable =
        new MicrometerQueueHealthMetrics(null, jobQueryService);

    assertDoesNotThrow(unavailable::afterStart);
    assertDoesNotThrow(unavailable::beforeStop);
    verifyNoInteractions(jobQueryService);
  }

  @Test
  void proxyConstructorHasNoOpLifecycle() {
    MicrometerQueueHealthMetrics proxy = new MicrometerQueueHealthMetrics();

    assertDoesNotThrow(proxy::beforeStart);
    assertDoesNotThrow(proxy::afterStart);
    assertDoesNotThrow(proxy::beforeStop);
    assertDoesNotThrow(proxy::afterStop);
  }

  @Test
  void repeatedStartDoesNotDuplicateAndStopRemovesEveryGauge() {
    metrics.afterStart();
    List<Meter> registered = List.copyOf(registry.getMeters());
    metrics.afterStart();

    assertEquals(GAUGE_COUNT, registered.size());
    assertEquals(registered, registry.getMeters());

    metrics.beforeStop();
    assertTrue(registry.getMeters().isEmpty());
    metrics.beforeStop();
    assertTrue(registry.getMeters().isEmpty());

    metrics.afterStart();
    assertEquals(GAUGE_COUNT, registry.getMeters().size());
    metrics.beforeStop();
    assertTrue(registry.getMeters().isEmpty());
    verifyNoInteractions(jobQueryService);
  }

  @Test
  void stopPreservesOtherMeters() {
    Meter unrelated = registry.counter("ratchet.jobs.completed");
    metrics.afterStart();

    metrics.beforeStop();

    assertEquals(List.of(unrelated), registry.getMeters());
  }

  @Test
  void deniedGaugesNeverQueryStore() {
    registry.config().meterFilter(MeterFilter.deny(id -> id.getName().startsWith("ratchet.queue")));

    metrics.afterStart();
    metrics.afterStart();
    assertTrue(registry.getMeters().isEmpty());
    readAllGauges();
    assertEquals("", registry.getMetersAsString());
    clock.advance(Duration.ofSeconds(30));
    readAllGauges();
    assertEquals("", registry.getMetersAsString());
    metrics.beforeStop();

    verifyNoInteractions(jobQueryService);
  }

  @Test
  void nullOldestPendingTimeHasZeroAge() {
    when(jobQueryService.getQueueHealth()).thenReturn(snapshot(14, 9, 1200, null));
    metrics.afterStart();

    assertEquals(0.0, gauge("ratchet.queue.oldest.pending.age").value());
  }

  @Test
  void futureOldestPendingTimeHasZeroAge() {
    when(jobQueryService.getQueueHealth()).thenReturn(snapshot(14, 9, 1200, NOW.plusSeconds(60)));
    metrics.afterStart();

    assertEquals(0.0, gauge("ratchet.queue.oldest.pending.age").value());
  }

  @Test
  void oldestPendingAgeAdvancesBetweenSnapshotRefreshes() {
    when(jobQueryService.getQueueHealth()).thenReturn(snapshot(14, 9, 1200, NOW.minusSeconds(90)));
    metrics.afterStart();

    assertEquals(90.0, gauge("ratchet.queue.oldest.pending.age").value());
    clock.advance(Duration.ofMillis(2500));
    assertEquals(92.5, gauge("ratchet.queue.oldest.pending.age").value());
    verify(jobQueryService).getQueueHealth();
  }

  private Gauge gauge(String name, String... tags) {
    return registry.get(name).tags(tags).gauge();
  }

  private void readAllGauges() {
    for (Meter meter : registry.getMeters()) {
      ((Gauge) meter).value();
    }
  }

  private static QueueHealthSnapshot snapshot(long pending, long ready, long p95, Instant oldest) {
    return new QueueHealthSnapshot(
        pending,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        ready,
        0.1,
        250,
        p95,
        oldest,
        Map.of(JobType.SINGLE, 10L, JobType.BATCH, 4L),
        Map.of(JobPriority.NORMAL, 9L, JobPriority.HIGH, 5L));
  }

  private static class MutableClock extends Clock {

    private Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(now, zone);
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
