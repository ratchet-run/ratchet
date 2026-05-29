package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.SignalStore;

@ExtendWith(MockitoExtension.class)
class JobTimeoutHandlerTest {

  private static final UUID JOB_ID = new UUID(0L, 42L);
  private static final long TIMEOUT_SEC = 30L;

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private PostExecutionHandler lifecycleFacade;
  @Mock private MetricsCollector metricsCollector;
  @Mock private SignalStore signalStore;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private TransactionSynchronizationRegistry txRegistry;

  private JobTimeoutHandler handler;

  @BeforeEach
  void setUp() {
    handler = newHandler(null, null, JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);
  }

  @Test
  void constructorRejectsNullClock() {
    assertThrows(
        NullPointerException.class,
        () ->
            new JobTimeoutHandler(
                jobCrudStore,
                jobRetryStore,
                jobBatchStatusStore,
                lifecycleFacade,
                80,
                60L,
                null,
                null,
                null,
                signalStore,
                metricsCollector,
                JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE));
  }

  @Test
  void hardTimeoutPostProcessingFailureRethrowsAfterCancellingFuture() throws Exception {
    when(jobCrudStore.findById(JOB_ID)).thenThrow(new IllegalStateException("store down"));
    FutureTask<Void> future = new FutureTask<>(() -> null);
    Method method =
        JobTimeoutHandler.class.getDeclaredMethod(
            "handleHardTimeoutById",
            UUID.class,
            java.util.concurrent.Future.class,
            Instant.class,
            long.class);
    method.setAccessible(true);

    InvocationTargetException thrown =
        assertThrows(
            InvocationTargetException.class,
            () -> method.invoke(handler, JOB_ID, future, Instant.EPOCH, TIMEOUT_SEC));

    assertInstanceOf(IllegalStateException.class, thrown.getCause());
    assertTrue(future.isCancelled());
  }

  @Test
  void retriesRemainingReschedulesInsteadOfDlq() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobRetryStore, times(1))
        .scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1));
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
  }

  @Test
  void hardTimeoutRetryAddsJitterToTimeoutDelay() {
    Instant now = Instant.parse("2026-05-09T12:00:00Z");
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.fixed(now, ZoneOffset.UTC));
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);
    ArgumentCaptor<Instant> retryTimeCaptor = ArgumentCaptor.forClass(Instant.class);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobRetryStore)
        .scheduleJobRetry(eq(JOB_ID), anyString(), retryTimeCaptor.capture(), eq(1));
    Instant baseRetryTime = now.plusSeconds(TIMEOUT_SEC);
    Instant maxRetryTime = baseRetryTime.plusMillis((TIMEOUT_SEC * 1000L) / 4);
    assertTrue(retryTimeCaptor.getValue().isAfter(baseRetryTime));
    assertTrue(!retryTimeCaptor.getValue().isAfter(maxRetryTime));
  }

  @Test
  void retriesExhaustedCasesToFailedAndEscalatesDlq() {
    JobEntity job = jobWithMaxRetries(0);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobBatchStatusStore, times(1))
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString());
    verify(lifecycleFacade, times(1)).handlePermanentFailure(eq(job), any());
  }

  @Test
  void hardTimeoutTerminalFailurePublishesFailedAndDlqEvents() {
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher);
    JobEntity job = jobWithMaxRetries(0);
    job.setBusinessKey("timeout-key");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(eventCaptor.capture());
    JobFailedEvent failedEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobFailedEvent.class::isInstance)
            .map(JobFailedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(JOB_ID, failedEvent.getJobId());
    assertEquals("timeout-key", failedEvent.getBusinessKey());
    assertEquals(1, failedEvent.getRetryAttempt());
    JobDlqEvent dlqEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobDlqEvent.class::isInstance)
            .map(JobDlqEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(JOB_ID, dlqEvent.getJobId());
    assertEquals(1, dlqEvent.getRetryAttempt());
  }

  @Test
  void racePathDoesNotEscalateToDlqWhenScheduleRetryLoses() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(false);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
  }

  @Test
  void incrementRetryReturnsMinusOneExitsCleanly() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(-1);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void missingJobExitsCleanly() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.empty());

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobRetryStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void signalTimeoutRetriesRemainingReschedulesInsteadOfDlq() {
    JobEntity job = waitingJobWithMaxRetries(3);
    job.setBackoffPolicy(BackoffPolicy.FIXED);
    job.setBackoffParamMs(2_500);
    Instant now = Instant.parse("2026-05-09T12:00:00Z");
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);
    ArgumentCaptor<Instant> retryTimeCaptor = ArgumentCaptor.forClass(Instant.class);

    handler.processSignalTimeout(job, now);

    verify(jobRetryStore, times(1))
        .scheduleJobRetry(eq(JOB_ID), anyString(), retryTimeCaptor.capture(), eq(1));
    assertEquals(now.plusMillis(2_500), retryTimeCaptor.getValue());
    assertEquals(JobStatus.PENDING, job.getStatus());
    assertEquals(1, job.getAttempts());
    assertEquals("Signal timeout exceeded for key: approval", job.getLastError());
    assertEquals(now.plusMillis(2_500), job.getScheduledTime());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void signalTimeoutRetryRescheduleDoesNotPublishTimedOutEvent() {
    JobTimeoutHandler eventHandler =
        newHandler(
            null,
            metricsCollector,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher,
            null);
    JobEntity job = waitingJobWithMaxRetries(3);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);

    eventHandler.processSignalTimeout(job, Instant.now());

    verify(eventPublisher, never()).publish(any());
    verify(metricsCollector, never()).signalTimedOut(any(), any(), anyString());
  }

  @Test
  void signalTimeoutRetriesExhaustedFailsAndEscalatesDlq() {
    JobEntity job = waitingJobWithMaxRetries(0);
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    handler.processSignalTimeout(job, now);

    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobBatchStatusStore, times(1))
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString());
    assertEquals(JobStatus.FAILED, job.getStatus());
    assertEquals(1, job.getAttempts());
    assertEquals("Signal timeout exceeded for key: approval", job.getLastError());
    verify(lifecycleFacade, times(1)).handlePermanentFailure(eq(job), any());
  }

  @Test
  void signalTimeoutPermanentFailureUsesSignalTimeoutException() {
    JobEntity job = waitingJobWithMaxRetries(0);
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);
    ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);

    handler.processSignalTimeout(job, now);

    verify(lifecycleFacade).handlePermanentFailure(eq(job), throwableCaptor.capture());
    assertInstanceOf(SignalTimeoutException.class, throwableCaptor.getValue());
    assertEquals(
        "Signal timeout exceeded for key: approval", throwableCaptor.getValue().getMessage());
  }

  @Test
  void scanSignalTimeoutsUsesConfiguredBatchLimit() {
    JobTimeoutHandler limitedHandler = newHandler(signalStore, metricsCollector, 17);
    when(signalStore.findTimedOutSignalJobs(any(Instant.class), eq(17))).thenReturn(List.of());

    limitedHandler.scanSignalTimeouts();

    verify(signalStore).findTimedOutSignalJobs(any(Instant.class), eq(17));
  }

  @Test
  void signalTimeoutPublishesTimedOutMetricWhenFailureIsApplied() {
    JobTimeoutHandler metricsHandler =
        newHandler(null, metricsCollector, JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);
    JobEntity job = waitingJobWithMaxRetries(0);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    metricsHandler.processSignalTimeout(job, Instant.now());

    verify(metricsCollector).signalTimedOut(JOB_ID, job.getPublicJobType(), "approval");
  }

  @Test
  void signalTimeoutEventPublishesAfterCommitWithConfiguredTimeout() {
    Instant createdAt = Instant.parse("2026-05-09T12:00:00Z");
    JobTimeoutHandler txHandler =
        newHandler(
            null,
            metricsCollector,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.fixed(createdAt.plusSeconds(31), ZoneOffset.UTC),
            eventPublisher,
            txRegistry);
    JobEntity job = waitingJobWithMaxRetries(0);
    job.setCreatedAt(createdAt);
    job.setSignalTimeout(createdAt.plusSeconds(30));
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    txHandler.processSignalTimeout(job, createdAt.plusSeconds(31));

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobSignalTimedOutEvent event =
        assertInstanceOf(JobSignalTimedOutEvent.class, eventCaptor.getValue());
    assertEquals(Duration.ofSeconds(30), event.getSignalTimeout());
  }

  @Test
  void signalTimeoutRacePathDoesNotEscalateToDlqWhenScheduleRetryLoses() {
    JobEntity job = waitingJobWithMaxRetries(3);
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(false);

    handler.processSignalTimeout(job, now);

    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void signalTimeoutIncrementRetryReturnsMinusOneExitsCleanly() {
    JobEntity job = waitingJobWithMaxRetries(3);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(-1);

    handler.processSignalTimeout(job, Instant.now());

    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  private JobEntity jobWithMaxRetries(int maxRetries) {
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setMaxRetries(maxRetries);
    job.setStatus(JobStatus.RUNNING);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    return job;
  }

  private JobEntity waitingJobWithMaxRetries(int maxRetries) {
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setMaxRetries(maxRetries);
    job.setStatus(JobStatus.WAITING);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setSignalKey("approval");
    job.setSignalTimeout(Instant.now().minusSeconds(1));
    job.setBackoffPolicy(BackoffPolicy.NONE);
    return job;
  }

  private JobTimeoutHandler newHandler(
      SignalStore signalStore, MetricsCollector metricsCollector, int signalTimeoutBatchSize) {
    return newHandler(signalStore, metricsCollector, signalTimeoutBatchSize, Clock.systemUTC());
  }

  private JobTimeoutHandler newHandler(
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      Clock clock) {
    return newHandler(signalStore, metricsCollector, signalTimeoutBatchSize, clock, null, null);
  }

  private JobTimeoutHandler newHandler(
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      Clock clock,
      InternalEventPublisher eventPublisher) {
    return newHandler(
        signalStore, metricsCollector, signalTimeoutBatchSize, clock, eventPublisher, null);
  }

  private JobTimeoutHandler newHandler(
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      Clock clock,
      InternalEventPublisher eventPublisher,
      TransactionSynchronizationRegistry txRegistry) {
    return new JobTimeoutHandler(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        80,
        60L,
        clock,
        eventPublisher,
        null,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        txRegistry);
  }
}
