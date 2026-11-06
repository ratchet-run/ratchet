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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.EventOutboxStore;
import run.ratchet.store.spi.JobStore;

class DurableCompletionTest {
  final Instant now = Instant.parse("2026-11-06T10:00:00Z");
  final Clock clock = Clock.fixed(now, ZoneOffset.UTC);
  final JobStore store =
      mock(
          JobStore.class,
          withSettings().extraInterfaces(EventOutboxStore.class).defaultAnswer(CALLS_REAL_METHODS));
  final AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
  final InternalEventPublisher publisher = new InternalEventPublisher();
  final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
  final List<Object> published = new ArrayList<>();
  final DurableTerminalEvents durable =
      new DurableTerminalEvents(
          RatchetOptions.builder().events(e -> e.durable(true)).build(),
          store,
          clock,
          publisher,
          registrar,
          mock(SingletonLeaseService.class));
  final DeadLetterService dlq =
      new DeadLetterService(
          null,
          store,
          store,
          null,
          publisher,
          mock(ErrorSanitizer.class),
          clock,
          registrar,
          durable);
  final WorkflowScheduler workflow = mock(WorkflowScheduler.class);
  final PostExecutionHandler handler =
      new PostExecutionHandler(
          mock(BatchService.class),
          workflow,
          dlq,
          mock(PollerScheduler.class),
          store,
          registrar,
          null,
          durable);

  DurableCompletionTest() {
    publisher.addListener(published::add);
    when(workflow.planCompletion(any(), anyBoolean())).thenReturn(WorkflowCompletionPlan.empty());
    when(store.commitCompletion(any())).thenReturn(new JobCompletionResult(true, null));
    when(registrar.registerAfterCommit(any()))
        .thenReturn(AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION);
    durable.start(executor);
    clearInvocations(store, executor);
  }

  JobEntity job() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setAttempts(1);
    job.setLastError("failed");
    return job;
  }

  JobCompletionPlan plan() {
    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(store).commitCompletion(plan.capture());
    return plan.getValue();
  }

  @Test
  void successRowsAndPublishedObjectsShareIdsAndClock() {
    assertTrue(handler.completeSuccess(job(), null, null, now, now, 5, 0));
    assertEquals(1, plan().outboxEvents().size());
    var row = plan().outboxEvents().get(0);
    var event = assertInstanceOf(JobCompletedEvent.class, published.get(0));
    assertEquals(event.getEventId(), row.eventId());
    assertEquals(7, row.eventId().version());
    assertEquals(now, row.createdAt());
    verify((EventOutboxStore) store, never()).deleteEvents(any());
    verify(executor).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  void terminalFailureRowsMatchOrderedPublishedPair() {
    assertTrue(handler.completeFailure(job(), JobStatus.RUNNING, false));
    assertEquals(2, plan().outboxEvents().size());
    assertInstanceOf(JobFailedEvent.class, published.get(0));
    assertInstanceOf(JobDlqEvent.class, published.get(1));
    for (int i = 0; i < 2; i++)
      assertEquals(
          ((AbstractJobSchedulerEvent) published.get(i)).getEventId(),
          plan().outboxEvents().get(i).eventId());
  }

  @Test
  void timeoutWithNoPublisherStillBuildsFailureAndDlqRows() {
    DurableTerminalEvents noPublisher =
        new DurableTerminalEvents(
            RatchetOptions.builder().events(e -> e.durable(true)).build(),
            store,
            clock,
            null,
            registrar,
            null);
    DeadLetterService noPublisherDlq =
        new DeadLetterService(null, store, store, null, null, null, clock, registrar, noPublisher);
    PostExecutionHandler noPublisherHandler =
        new PostExecutionHandler(
            mock(BatchService.class),
            workflow,
            noPublisherDlq,
            mock(PollerScheduler.class),
            store,
            registrar,
            null,
            noPublisher);
    JobEntity source = job();
    assertTrue(
        noPublisherHandler.completeTimeoutFailure(
            source,
            JobStatus.RUNNING,
            false,
            null,
            List.of(
                new JobFailedEvent(
                    source.getId(), null, null, JobType.SINGLE, null, null, "failed", 1))));
    assertEquals(2, plan().outboxEvents().size());
    assertTrue(published.isEmpty());
    verifyNoInteractions(executor);
  }

  @ParameterizedTest
  @EnumSource(
      value = AfterCommitRegistrar.Result.class,
      names = {"ROLLBACK_ONLY", "REGISTRATION_FAILED", "REGISTERED"})
  void suppressedOrNotYetCommittedCallbackNeverQueuesDeletes(AfterCommitRegistrar.Result result) {
    when(registrar.registerAfterCommit(any())).thenReturn(result);
    assertTrue(handler.completeSuccess(job(), null, null, now, now, 1, 0));
    assertFalse(plan().outboxEvents().isEmpty());
    assertTrue(published.isEmpty());
    verifyNoInteractions(executor);
    verify((EventOutboxStore) store, never()).deleteEvents(any());
  }

  @Test
  void disabledCompletionLeavesPlanEmptyAndUsesExistingPublication() {
    DurableTerminalEvents disabled =
        new DurableTerminalEvents(
            RatchetOptions.defaults(), store, clock, publisher, registrar, null);
    PostExecutionHandler handler =
        new PostExecutionHandler(
            mock(BatchService.class),
            workflow,
            dlq,
            mock(PollerScheduler.class),
            store,
            registrar,
            null,
            disabled);
    assertTrue(handler.completeSuccess(job(), null, null, now, now, 1, 0));
    assertTrue(plan().outboxEvents().isEmpty());
    verify(workflow).publishTerminalEvent(any(JobCompletedEvent.class));
    verifyNoInteractions(executor);
  }

  @Test
  void losingCompletionNeverPublishesOrEnqueues() {
    when(store.commitCompletion(any())).thenReturn(new JobCompletionResult(false, null));
    assertFalse(handler.completeSuccess(job(), null, null, now, now, 1, 0));
    assertTrue(published.isEmpty());
    verifyNoInteractions(executor, registrar);
  }
}
