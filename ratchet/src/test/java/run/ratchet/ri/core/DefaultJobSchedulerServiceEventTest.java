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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobPausedEvent;
import run.ratchet.api.event.JobResumedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobSchedulerServiceEventTest {

  private static final UUID JOB_ID = new UUID(0L, 90L);
  private static final UUID REPLACEMENT_ID = new UUID(0L, 91L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-15T10:15:30Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private InternalEventPublisher eventPublisher;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobPauseStore jobPauseStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private run.ratchet.store.spi.RecurringJobStore recurringJobStore;
  @Mock private JobWakeupService wakeupService;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private DefaultJobCreationService jobCreationService;
  @Mock private SignalStore signalStore;
  @Mock private MetricsCollector metricsCollector;

  private DefaultJobSchedulerService service;

  public static void noopTask() {}

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobSchedulerService(
            eventPublisher,
            jobBatchStatusStore,
            jobPauseStore,
            jobRetryStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            recurringJobStore,
            wakeupService,
            recurringScheduler,
            null,
            jobCreationService,
            null,
            null,
            signalStore,
            null,
            metricsCollector,
            FIXED_CLOCK);
  }

  @ParameterizedTest
  @EnumSource(
      value = JobStatus.class,
      names = {"PENDING", "PAUSED", "WAITING"})
  void replacePublishesCancelledEventForServiceOwnedCancellation(JobStatus previousStatus) {
    JobEntity job = job(previousStatus, JobExecutionType.SINGLE);
    job.setSignalKey("approval-" + previousStatus.name());
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobCreationService.submit(any(DefaultJobBuilder.class))).thenReturn(() -> REPLACEMENT_ID);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), isNull()))
        .thenAnswer(inv -> inv.getArgument(1) == previousStatus);
    when(jobCrudStore.save(any(JobEntity.class))).thenReturn(job);

    assertEquals(
        REPLACEMENT_ID,
        service
            .replace(JOB_ID, Duration.ZERO, DefaultJobSchedulerServiceEventTest::noopTask, null)
            .id());

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JOB_ID, event.getJobId());
    assertEquals(previousStatus.name(), event.getPreviousStatus());
    assertEquals(FIXED_NOW, event.getTimestamp());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    if (previousStatus == JobStatus.WAITING) {
      verify(metricsCollector).signalCancelled(JOB_ID, job.getPublicJobType(), job.getSignalKey());
    } else {
      verify(metricsCollector, never()).signalCancelled(any(), any(), any());
    }
  }

  @Test
  void replaceRunningCancellationPublishesCancelledEventAfterStateChange() {
    JobEntity job = job(JobStatus.RUNNING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobCreationService.submit(any(DefaultJobBuilder.class))).thenReturn(() -> REPLACEMENT_ID);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), isNull()))
        .thenAnswer(inv -> inv.getArgument(1) == JobStatus.RUNNING);
    when(jobCrudStore.save(any(JobEntity.class))).thenReturn(job);

    assertEquals(
        REPLACEMENT_ID,
        service
            .replace(JOB_ID, Duration.ZERO, DefaultJobSchedulerServiceEventTest::noopTask, null)
            .id());

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JobStatus.RUNNING.name(), event.getPreviousStatus());
    assertEquals(FIXED_NOW, event.getTimestamp());
    verify(metricsCollector, never()).signalCancelled(any(), any(), any());
  }

  @Test
  void replaceRecurringMasterUsesTerminalCancelAndRecordsCanceledSupersession() {
    JobEntity job = job(JobStatus.PENDING, JobExecutionType.RECURRING);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobCreationService.submit(any(DefaultJobBuilder.class))).thenReturn(() -> REPLACEMENT_ID);
    when(jobTerminalStore.cancelJob(JOB_ID)).thenReturn(true);
    when(jobCrudStore.save(any(JobEntity.class))).thenAnswer(inv -> inv.getArgument(0));

    assertEquals(
        REPLACEMENT_ID,
        service
            .replace(JOB_ID, Duration.ZERO, DefaultJobSchedulerServiceEventTest::noopTask, null)
            .id());

    verify(jobTerminalStore).cancelJob(JOB_ID);
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(), any(), any(), any());

    ArgumentCaptor<JobEntity> saved = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).save(saved.capture());
    assertEquals(JobStatus.CANCELED, saved.getValue().getStatus());
    assertEquals(REPLACEMENT_ID, saved.getValue().getSupersededBy());

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JobStatus.PENDING.name(), event.getPreviousStatus());
    assertCommonJobEvent(event, JobType.RECURRING);
  }

  @Test
  void cancelJobOnRecurringMasterRoutesToRecurringStore() {
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), isNull()))
        .thenReturn(false);
    when(recurringJobStore.getRecurring(JOB_ID)).thenReturn(Optional.of(recurringDef(false)));
    when(recurringJobStore.cancelRecurringAndArchive(
            JOB_ID, run.ratchet.store.spi.RecurringJobStore.ArchiveReason.CANCELED))
        .thenReturn(true);

    assertTrue(service.cancelJob(JOB_ID));

    verify(recurringJobStore)
        .cancelRecurringAndArchive(
            JOB_ID, run.ratchet.store.spi.RecurringJobStore.ArchiveReason.CANCELED);
  }

  @Test
  void cancelJobRunningCancellationPublishesCancelledEventAfterStateChange() {
    JobEntity job = job(JobStatus.RUNNING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), isNull()))
        .thenAnswer(inv -> inv.getArgument(1) == JobStatus.RUNNING);

    assertTrue(service.cancelJob(JOB_ID));

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JobStatus.RUNNING.name(), event.getPreviousStatus());
    assertEquals(FIXED_NOW, event.getTimestamp());
    assertEquals("business-90", event.getBusinessKey());
  }

  @Test
  void cancelJobRecurringPublishesCancelledEvent() {
    // Recurring master cancellation goes through RecurringJobStore, not the CAS chain. The
    // event must still fire so audit/monitoring sees the same shape regardless of source state.
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.empty());
    when(recurringJobStore.getRecurring(JOB_ID)).thenReturn(Optional.of(recurringDef(false)));
    when(recurringJobStore.cancelRecurringAndArchive(
            JOB_ID, run.ratchet.store.spi.RecurringJobStore.ArchiveReason.CANCELED))
        .thenReturn(true);

    assertTrue(service.cancelJob(JOB_ID));

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(JobType.RECURRING, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals(JobStatus.PENDING.name(), event.getPreviousStatus());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }

  @Test
  void cancelJobRecurringPausedReportsPausedAsPreviousStatus() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.empty());
    when(recurringJobStore.getRecurring(JOB_ID)).thenReturn(Optional.of(recurringDef(true)));
    when(recurringJobStore.cancelRecurringAndArchive(
            JOB_ID, run.ratchet.store.spi.RecurringJobStore.ArchiveReason.CANCELED))
        .thenReturn(true);

    assertTrue(service.cancelJob(JOB_ID));

    JobCancelledEvent event = published(JobCancelledEvent.class);
    assertEquals(JobStatus.PAUSED.name(), event.getPreviousStatus());
  }

  @Test
  void retryJobPublishesRetryingEventAndWakesPoller() {
    JobEntity job = job(JobStatus.FAILED, JobExecutionType.SINGLE);
    job.setLastError("boom");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.resetFailedToPending(JOB_ID)).thenReturn(true);

    assertTrue(service.retryJob(JOB_ID));

    JobRetryingEvent event = published(JobRetryingEvent.class);
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals(FIXED_NOW, event.getTimestamp());
    assertEquals("boom", event.getErrorMessage());
    assertEquals(1, event.getRetryAttempt());
    assertEquals(FIXED_NOW, event.getScheduledTime());
    verify(wakeupService)
        .notifyIfNeeded(JobExecutionType.SINGLE, JobPriority.HIGH, Duration.ZERO, null);
  }

  @Test
  void pauseJobPendingPublishesPausedEvent() {
    JobEntity job = job(JobStatus.PENDING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobPauseStore.transitionToPaused(JOB_ID, JobStatus.PENDING)).thenReturn(true);

    assertTrue(service.pauseJob(JOB_ID));

    JobPausedEvent event = published(JobPausedEvent.class);
    assertCommonJobEvent(event, JobType.SINGLE);
  }

  @Test
  void pauseJobRecurringPublishesPausedEvent() {
    when(recurringJobStore.getRecurring(JOB_ID)).thenReturn(Optional.of(recurringDef(false)));
    when(recurringJobStore.pauseRecurring(JOB_ID)).thenReturn(true);

    assertTrue(service.pauseJob(JOB_ID));

    JobPausedEvent event = published(JobPausedEvent.class);
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(JobType.RECURRING, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }

  @Test
  void pauseJobAlreadyPausedDoesNotRepublishPausedEvent() {
    JobEntity job = job(JobStatus.PAUSED, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertTrue(service.pauseJob(JOB_ID));

    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void resumeJobPausedPublishesResumedEvent() {
    JobEntity job = job(JobStatus.PAUSED, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobPauseStore.transitionFromPausedAtomic(JOB_ID)).thenReturn(JobStatus.PENDING);

    assertTrue(service.resumeJob(JOB_ID));

    JobResumedEvent event = published(JobResumedEvent.class);
    assertCommonJobEvent(event, JobType.SINGLE);
    verify(recurringScheduler).kick();
  }

  @Test
  void resumeJobRecurringPublishesResumedEvent() {
    when(recurringJobStore.getRecurring(JOB_ID)).thenReturn(Optional.of(recurringDef(true)));
    when(recurringJobStore.resumeRecurring(JOB_ID)).thenReturn(true);

    assertTrue(service.resumeJob(JOB_ID));

    JobResumedEvent event = published(JobResumedEvent.class);
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(JobType.RECURRING, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    verify(recurringScheduler).kick();
  }

  @Test
  void resumeJobNotPausedDoesNotPublishResumedEvent() {
    JobEntity job = job(JobStatus.PENDING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertFalse(service.resumeJob(JOB_ID));

    verify(eventPublisher, never()).publish(any());
  }

  private static run.ratchet.store.spi.RecurringJobDefinition recurringDef(boolean paused) {
    return new run.ratchet.store.spi.RecurringJobDefinition(
        JOB_ID,
        "0 * * * * ?",
        "UTC",
        FIXED_NOW.plusSeconds(60),
        paused,
        paused ? FIXED_NOW : null,
        JobPriority.HIGH.ordinal(),
        0,
        run.ratchet.api.BackoffPolicy.NONE,
        0,
        0,
        null,
        null,
        null,
        "business-90",
        null,
        null,
        FIXED_NOW,
        null,
        false);
  }

  private static JobEntity job(JobStatus status, JobExecutionType type) {
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setStatus(status);
    job.setJobType(type);
    job.setBusinessKey("business-90");
    job.setPriority(JobPriority.HIGH);
    job.setPickedBy("node-a");
    return job;
  }

  private <T> T published(Class<T> type) {
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    return assertInstanceOf(type, eventCaptor.getValue());
  }

  private static void assertCommonJobEvent(
      run.ratchet.api.event.AbstractJobSchedulerEvent event, JobType jobType) {
    assertEquals(JOB_ID, event.getJobId());
    assertEquals("business-90", event.getBusinessKey());
    assertEquals(jobType, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }
}
