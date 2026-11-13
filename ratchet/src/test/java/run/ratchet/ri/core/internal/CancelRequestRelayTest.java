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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.store.spi.JobCrudStore;

@ExtendWith(MockitoExtension.class)
class CancelRequestRelayTest {
  private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
  private static final Instant CUTOFF = NOW.minusSeconds(5);
  @Mock private JobTimeoutHandler timeoutHandler;
  @Mock private CancelStatusReader statusReader;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private ScheduledExecutorService executor;
  @Mock private ScheduledFuture<?> future;
  @Captor private ArgumentCaptor<Consumer<Object>> listener;
  @Captor private ArgumentCaptor<Collection<UUID>> reads;
  private CancelRequestRelay relay;

  @BeforeEach
  void setUp() {
    relay =
        new CancelRequestRelay(
            timeoutHandler, statusReader, eventPublisher, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @Test
  void start_registersListenerAndSchedulesFixedDelayTick() {
    // Starting registers a listener and schedules a five-second fixed-delay poll.
    relay.start(executor);
    verify(eventPublisher).addListener(any());
    verify(executor)
        .scheduleWithFixedDelay(any(Runnable.class), eq(5L), eq(5L), eq(TimeUnit.SECONDS));
  }

  @Test
  void stop_removesTheSameListenerAndCancelsTheTick() {
    // Stopping removes the registered listener and cancels the poll without interruption.
    doReturn(future)
        .when(executor)
        .scheduleWithFixedDelay(any(Runnable.class), eq(5L), eq(5L), eq(TimeUnit.SECONDS));
    relay.start(executor);
    verify(eventPublisher).addListener(listener.capture());
    relay.stop();
    verify(eventPublisher).removeListener(listener.getValue());
    verify(future).cancel(false);
  }

  @Test
  void runningCancelEvent_requestsUserCancelAtOnce() {
    // A committed running cancellation immediately signals the local body.
    UUID id = UUID.randomUUID();
    relay.start(executor);
    verify(eventPublisher).addListener(listener.capture());
    listener.getValue().accept(cancelEvent(id, "RUNNING"));
    verify(timeoutHandler).requestUserCancel(id);
  }

  @Test
  void nonRunningCancelEventAndOtherEvents_areIgnored() {
    // Pending cancellations and unrelated events never signal a running body.
    relay.start(executor);
    verify(eventPublisher).addListener(listener.capture());
    listener.getValue().accept(cancelEvent(UUID.randomUUID(), "PENDING"));
    listener.getValue().accept(new Object());
    verifyNoInteractions(timeoutHandler);
  }

  @Test
  void tick_withNoOldAttempts_readsNothing() {
    // An empty snapshot avoids the status store and uses the exact age cutoff.
    when(timeoutHandler.localAttemptsStartedBefore(CUTOFF)).thenReturn(List.of());
    relay.tick();
    verify(timeoutHandler).localAttemptsStartedBefore(CUTOFF);
    verifyNoInteractions(statusReader);
  }

  @Test
  void tick_requestsCancelOnlyForCanceledStatus() {
    // Only an explicit canceled row signals cancellation; running and missing rows do nothing.
    JobAttemptControl a = attempt(UUID.randomUUID());
    JobAttemptControl b = attempt(UUID.randomUUID());
    JobAttemptControl c = attempt(UUID.randomUUID());
    when(timeoutHandler.localAttemptsStartedBefore(CUTOFF)).thenReturn(List.of(a, b, c));
    when(statusReader.findStatuses(any()))
        .thenReturn(Map.of(a.jobId(), JobStatus.CANCELED, b.jobId(), JobStatus.RUNNING));
    relay.tick();
    verify(timeoutHandler).requestUserCancel(a.jobId());
    verify(timeoutHandler, never()).requestUserCancel(b.jobId());
    verify(timeoutHandler, never()).requestUserCancel(c.jobId());
  }

  @Test
  void tick_readsEachJobOnce() {
    // Overlapping attempts of one job share one status read.
    UUID id = UUID.randomUUID();
    when(timeoutHandler.localAttemptsStartedBefore(CUTOFF))
        .thenReturn(List.of(attempt(id), attempt(id)));
    when(statusReader.findStatuses(any())).thenReturn(Map.of());
    relay.tick();
    verify(statusReader).findStatuses(reads.capture());
    assertEquals(List.of(id), List.copyOf(reads.getValue()));
  }

  @Test
  void tick_chunksReadsAtTheStatusLimit() {
    // A large snapshot is read in bounded chunks with every job read exactly once.
    List<JobAttemptControl> attempts =
        IntStream.range(0, 2 * JobCrudStore.STATUS_READ_LIMIT + 1)
            .mapToObj(i -> attempt(new UUID(0, i)))
            .toList();
    when(timeoutHandler.localAttemptsStartedBefore(CUTOFF)).thenReturn(attempts);
    when(statusReader.findStatuses(any())).thenReturn(Map.of());
    relay.tick();
    verify(statusReader, times(3)).findStatuses(reads.capture());
    assertEquals(
        List.of(JobCrudStore.STATUS_READ_LIMIT, JobCrudStore.STATUS_READ_LIMIT, 1),
        reads.getAllValues().stream().map(Collection::size).toList());
    assertEquals(
        attempts.stream().map(JobAttemptControl::jobId).collect(Collectors.toSet()),
        reads.getAllValues().stream().flatMap(Collection::stream).collect(Collectors.toSet()));
  }

  @Test
  void tick_storeFailureIsSwallowedAndRetriedNextTick() {
    // A failed read leaves the poll alive so the next tick can deliver the cancellation.
    UUID id = UUID.randomUUID();
    when(timeoutHandler.localAttemptsStartedBefore(CUTOFF)).thenReturn(List.of(attempt(id)));
    when(statusReader.findStatuses(any()))
        .thenThrow(new IllegalStateException("store down"))
        .thenReturn(Map.of(id, JobStatus.CANCELED));
    assertDoesNotThrow(relay::tick);
    verify(timeoutHandler, never()).requestUserCancel(any());
    assertDoesNotThrow(relay::tick);
    verify(statusReader, times(2)).findStatuses(any());
    verify(timeoutHandler).requestUserCancel(id);
  }

  private JobAttemptControl attempt(UUID id) {
    return new JobTimeoutHandler().newAttempt(id, 30, NOW.minusSeconds(10), 0, 7, "node-1");
  }

  private JobCancelledEvent cancelEvent(UUID id, String previous) {
    return new JobCancelledEvent(id, null, null, null, null, null, NOW, previous, null);
  }
}
