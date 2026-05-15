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
  void replaceRunningCancellationLeavesEventToExecutor() {
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

    verify(eventPublisher, never()).publish(any());
    verify(metricsCollector, never()).signalCancelled(any(), any(), any());
  }

  @Test
  void cancelJobRunningCancellationLeavesEventToExecutor() {
    JobEntity job = job(JobStatus.RUNNING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), any(JobStatus.class), eq(JobStatus.CANCELED), isNull()))
        .thenAnswer(inv -> inv.getArgument(1) == JobStatus.RUNNING);

    assertTrue(service.cancelJob(JOB_ID));

    verify(eventPublisher, never()).publish(any());
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
    JobEntity job = job(JobStatus.PENDING, JobExecutionType.RECURRING);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobPauseStore.pauseRecurring(JOB_ID)).thenReturn(true);

    assertTrue(service.pauseJob(JOB_ID));

    JobPausedEvent event = published(JobPausedEvent.class);
    assertCommonJobEvent(event, JobType.RECURRING);
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
    JobEntity job = job(JobStatus.PAUSED, JobExecutionType.RECURRING);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobPauseStore.resumeRecurring(JOB_ID)).thenReturn(true);

    assertTrue(service.resumeJob(JOB_ID));

    JobResumedEvent event = published(JobResumedEvent.class);
    assertCommonJobEvent(event, JobType.RECURRING);
    verify(recurringScheduler).kick();
  }

  @Test
  void resumeJobNotPausedDoesNotPublishResumedEvent() {
    JobEntity job = job(JobStatus.PENDING, JobExecutionType.SINGLE);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));

    assertFalse(service.resumeJob(JOB_ID));

    verify(eventPublisher, never()).publish(any());
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
