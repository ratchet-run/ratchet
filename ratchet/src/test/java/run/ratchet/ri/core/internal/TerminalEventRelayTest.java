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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobType;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventType;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.EventOutboxStore;

class TerminalEventRelayTest {
  final EventOutboxStore store = mock(EventOutboxStore.class);
  final Instant now = Instant.parse("2026-11-06T12:00:00Z");
  final Clock clock = Clock.fixed(now, ZoneOffset.UTC);
  final BatchStore batches = mock(BatchStore.class);
  final SingletonLeaseService leases = mock(SingletonLeaseService.class);
  final InternalEventPublisher publisher = mock(InternalEventPublisher.class);
  final RatchetOptions options =
      RatchetOptions.builder().node(n -> n.orphanGraceSeconds(10)).build();
  final TerminalEventRelay relay =
      new TerminalEventRelay(store, batches, leases, publisher, options, clock);

  @Test
  void heldLeaseSkipsAllReads() {
    when(leases.tryAcquire(anyString(), any())).thenReturn(Optional.empty());
    relay.relayNow();
    verifyNoInteractions(store, batches, publisher);
  }

  @Test
  void appClockCutoffDurationFillPublishBeforeDeleteAndTenBatchBound() {
    when(leases.tryAcquire(eq("terminalEventRelay"), any()))
        .thenReturn(Optional.of(new SingletonLease(null, "terminalEventRelay", "test-node")));
    OutboxEvent row =
        new OutboxEvent(
            UUID.randomUUID(),
            OutboxEventType.JOB_COMPLETED,
            UUID.randomUUID(),
            null,
            null,
            JobType.BATCH,
            null,
            "dead",
            now.minusSeconds(50),
            null,
            null,
            0,
            now.minusSeconds(50));
    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setTotalDurationMs(123L);
    when(batches.findBatchMetrics(row.jobId())).thenReturn(Optional.of(metrics));
    when(store.findEventsCreatedBefore(now.minusSeconds(10), 100)).thenReturn(List.of(row));
    relay.relayNow();
    verify(store, times(10)).findEventsCreatedBefore(now.minusSeconds(10), 100);
    ArgumentCaptor<JobCompletedEvent> event = ArgumentCaptor.forClass(JobCompletedEvent.class);
    var order = inOrder(store, publisher);
    for (int i = 0; i < 10; i++) {
      order.verify(store).findEventsCreatedBefore(now.minusSeconds(10), 100);
      order.verify(publisher).publish(event.capture());
      order.verify(store).deleteEvents(List.of(row.eventId()));
    }
    assertEquals(123L, event.getValue().getExecutionTimeMs());
    assertEquals(row.eventId(), event.getValue().getEventId());
    relay.stop();
    relay.relayNow();
    verify(store, times(10)).findEventsCreatedBefore(any(), anyInt());
  }

  @Test
  void rowThatCannotBecomeAnEventIsDroppedWithoutBlockingLaterRows() {
    when(leases.tryAcquire(eq("terminalEventRelay"), any()))
        .thenReturn(Optional.of(new SingletonLease(null, "terminalEventRelay", "test-node")));
    OutboxEvent invalid = completedRow(-5L, now.minusSeconds(60));
    OutboxEvent valid = completedRow(7L, now.minusSeconds(50));
    when(store.findEventsCreatedBefore(now.minusSeconds(10), 100))
        .thenReturn(List.of(invalid, valid), List.of());

    relay.relayNow();

    ArgumentCaptor<JobCompletedEvent> event = ArgumentCaptor.forClass(JobCompletedEvent.class);
    verify(publisher).publish(event.capture());
    assertEquals(valid.eventId(), event.getValue().getEventId());
    verify(store).deleteEvents(List.of(invalid.eventId(), valid.eventId()));
  }

  private static OutboxEvent completedRow(Long executionTimeMs, Instant createdAt) {
    return new OutboxEvent(
        UUID.randomUUID(),
        OutboxEventType.JOB_COMPLETED,
        UUID.randomUUID(),
        null,
        null,
        JobType.SINGLE,
        null,
        "dead",
        createdAt,
        executionTimeMs,
        null,
        0,
        createdAt);
  }
}
