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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
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
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobExecutionTimedOutEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.ri.core.internal.PostExecutionHandler.TerminalTimeoutTransition;
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
  private TerminalTimeoutTransition terminalTimeoutTransition;

  @BeforeEach
  void setUp() {
    lenient()
        .when(lifecycleFacade.handleTimeoutTransition(any(), anyBoolean(), any(Supplier.class)))
        .thenAnswer(
            invocation -> {
              Optional<?> terminalJob =
                  (Optional<?>) invocation.getArgument(2, Supplier.class).get();
              terminalJob.ifPresent(
                  outcome -> terminalTimeoutTransition = (TerminalTimeoutTransition) outcome);
              return terminalJob.isPresent();
            });
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
            "handleHardTimeoutById", UUID.class, Future.class, Instant.class, long.class);
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
    Instant now = Instant.parse("2026-05-09T12:00:00Z");
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.fixed(now, ZoneOffset.UTC),
            eventPublisher,
            txRegistry);
    JobEntity job = jobWithMaxRetries(3);
    job.setBusinessKey("timeout-key");
    job.setPickedBy("node-a");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC, Duration.ofSeconds(31));

    verify(jobRetryStore, times(1))
        .scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1));
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(eventCaptor.capture());
    JobExecutionTimedOutEvent timedOutEvent =
        assertInstanceOf(JobExecutionTimedOutEvent.class, eventCaptor.getAllValues().get(0));
    assertEquals(JOB_ID, timedOutEvent.getJobId());
    assertEquals("timeout-key", timedOutEvent.getBusinessKey());
    assertEquals("node-a", timedOutEvent.getNodeId());
    assertEquals(now, timedOutEvent.getTimestamp());
    assertEquals(Duration.ofSeconds(TIMEOUT_SEC), timedOutEvent.getExecutionTimeout());
    assertEquals(Duration.ofSeconds(31), timedOutEvent.getElapsedTime());
    assertEquals(1, timedOutEvent.getRetryAttempt());
    JobRetryingEvent retryingEvent =
        assertInstanceOf(JobRetryingEvent.class, eventCaptor.getAllValues().get(1));
    assertEquals(1, retryingEvent.getRetryAttempt());
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
    verify(lifecycleFacade)
        .handleTimeoutTransition(any(TimeoutException.class), eq(false), any(Supplier.class));
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void terminalHardTimeoutCarriesTimedOutThenFailedForTheLifecycleTransaction() {
    Instant now = Instant.parse("2026-05-09T12:00:00Z");
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.fixed(now, ZoneOffset.UTC),
            eventPublisher,
            txRegistry);
    JobEntity job = jobWithMaxRetries(0);
    job.setBusinessKey("timeout-key");
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);
    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC, Duration.ofSeconds(31));

    verify(eventPublisher, never()).publish(any());
    assertEquals(job, terminalTimeoutTransition.job());
    assertEquals(2, terminalTimeoutTransition.eventsBeforeDlq().size());
    JobExecutionTimedOutEvent timedOutEvent =
        assertInstanceOf(
            JobExecutionTimedOutEvent.class, terminalTimeoutTransition.eventsBeforeDlq().get(0));
    assertEquals(JOB_ID, timedOutEvent.getJobId());
    assertEquals(Duration.ofSeconds(TIMEOUT_SEC), timedOutEvent.getExecutionTimeout());
    assertEquals(Duration.ofSeconds(31), timedOutEvent.getElapsedTime());
    assertEquals(1, timedOutEvent.getRetryAttempt());
    JobFailedEvent failedEvent =
        assertInstanceOf(JobFailedEvent.class, terminalTimeoutTransition.eventsBeforeDlq().get(1));
    assertEquals(JOB_ID, failedEvent.getJobId());
    assertEquals("timeout-key", failedEvent.getBusinessKey());
    assertEquals(1, failedEvent.getRetryAttempt());
  }

  @Test
  void racePathDoesNotEscalateToDlqWhenScheduleRetryLoses() {
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher);
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(false);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void terminalRacePathDoesNotPublishWhenStatusCasLoses() {
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher);
    JobEntity job = jobWithMaxRetries(0);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(false);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(eventPublisher, never()).publish(any());
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
  void hardTimeoutDoesNotMutateStateBeforeEnteringRequiresNewBoundary() {
    doReturn(false)
        .when(lifecycleFacade)
        .handleTimeoutTransition(any(TimeoutException.class), eq(false), any(Supplier.class));

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(lifecycleFacade)
        .handleTimeoutTransition(any(TimeoutException.class), eq(false), any(Supplier.class));
    verify(jobCrudStore, never()).findById(any(UUID.class));
    verify(jobRetryStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(jobRetryStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(), any(), any(), any());
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
  void signalTimeoutRetryReschedulePublishesRetryingEventButNotTerminalTimedOutEvent() {
    JobTimeoutHandler eventHandler =
        newHandler(
            null,
            metricsCollector,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher,
            txRegistry);
    JobEntity job = waitingJobWithMaxRetries(3);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);

    eventHandler.processSignalTimeout(job, Instant.now());

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    JobRetryingEvent retryingEvent =
        assertInstanceOf(JobRetryingEvent.class, eventCaptor.getValue());
    assertEquals(JOB_ID, retryingEvent.getJobId());
    assertEquals(1, retryingEvent.getRetryAttempt());
    assertEquals(job.getScheduledTime(), retryingEvent.getScheduledTime());
    verify(metricsCollector, never()).signalTimedOut(any(), any(), anyString());
  }

  @Test
  void softTimeoutWarningDoesNotPublishAnExecutionTimedOutEvent() throws Exception {
    Instant start = Instant.parse("2026-05-09T12:00:00Z");
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.fixed(start.plusSeconds(24), ZoneOffset.UTC),
            eventPublisher);
    FutureTask<Void> future = new FutureTask<>(() -> null);
    Method method =
        JobTimeoutHandler.class.getDeclaredMethod(
            "handleSoftTimeoutById",
            UUID.class,
            Future.class,
            AtomicBoolean.class,
            Instant.class,
            long.class);
    method.setAccessible(true);

    method.invoke(handler, JOB_ID, future, new AtomicBoolean(), start, TIMEOUT_SEC);

    verify(eventPublisher, never()).publish(any());
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
    verify(lifecycleFacade)
        .handleTimeoutTransition(any(SignalTimeoutException.class), eq(true), any(Supplier.class));
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void signalTimeoutPermanentFailureUsesSignalTimeoutException() {
    JobEntity job = waitingJobWithMaxRetries(0);
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);
    handler.processSignalTimeout(job, now);

    ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(lifecycleFacade)
        .handleTimeoutTransition(throwableCaptor.capture(), eq(true), any(Supplier.class));
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
  void scanSignalTimeoutsSkippedWhenSingletonLeaseNotHeld() {
    SingletonLeaseService leaseService = org.mockito.Mockito.mock(SingletonLeaseService.class);
    when(leaseService.tryAcquire(anyString(), any(Duration.class))).thenReturn(Optional.empty());
    JobTimeoutHandler leasedHandler = newLeasedHandler(leaseService);

    leasedHandler.scanSignalTimeouts();

    verify(signalStore, never()).findTimedOutSignalJobs(any(Instant.class), anyInt());
    verify(jobRetryStore, never()).incrementRetryAttempt(any(UUID.class));
  }

  @Test
  void scanSignalTimeoutsRunsWhenSingletonLeaseGranted() {
    SingletonLeaseService leaseService = org.mockito.Mockito.mock(SingletonLeaseService.class);
    when(leaseService.tryAcquire(anyString(), any(Duration.class)))
        .thenReturn(Optional.of(new SingletonLease(null, "signalTimeoutScan", "node-1")));
    JobTimeoutHandler leasedHandler = newLeasedHandler(leaseService);
    when(signalStore.findTimedOutSignalJobs(any(Instant.class), anyInt())).thenReturn(List.of());

    leasedHandler.scanSignalTimeouts();

    verify(signalStore).findTimedOutSignalJobs(any(Instant.class), anyInt());
  }

  @Test
  void scanSignalTimeoutsIsolatesPerJobFailuresAndProcessesRemainingJobs() {
    JobTimeoutHandler scanHandler =
        newHandler(
            signalStore, metricsCollector, JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);
    UUID poisonedId = new UUID(0L, 1L);
    UUID survivorId = new UUID(0L, 2L);
    JobEntity poisoned = waitingJob(poisonedId, 0);
    JobEntity survivor = waitingJob(survivorId, 0);
    when(signalStore.findTimedOutSignalJobs(any(Instant.class), anyInt()))
        .thenReturn(List.of(poisoned, survivor));
    when(jobRetryStore.incrementRetryAttempt(poisonedId))
        .thenThrow(new IllegalStateException("store down"));
    when(jobRetryStore.incrementRetryAttempt(survivorId)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(survivorId), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    scanHandler.scanSignalTimeouts();

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(
            eq(survivorId), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString());
    verify(lifecycleFacade, times(2))
        .handleTimeoutTransition(any(SignalTimeoutException.class), eq(true), any(Supplier.class));
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
  void terminalSignalTimeoutCarriesTimedOutThenFailedUsingScanTime() {
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
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    Instant scanTime = createdAt.plusSeconds(31);
    txHandler.processSignalTimeout(job, scanTime);

    verify(eventPublisher, never()).publish(any());
    assertEquals(job, terminalTimeoutTransition.job());
    assertEquals(2, terminalTimeoutTransition.eventsBeforeDlq().size());
    JobSignalTimedOutEvent timedOutEvent =
        assertInstanceOf(
            JobSignalTimedOutEvent.class, terminalTimeoutTransition.eventsBeforeDlq().get(0));
    assertEquals(Duration.ofSeconds(30), timedOutEvent.getSignalTimeout());
    assertEquals(scanTime, timedOutEvent.getTimestamp());
    JobFailedEvent failedEvent =
        assertInstanceOf(JobFailedEvent.class, terminalTimeoutTransition.eventsBeforeDlq().get(1));
    assertEquals(scanTime, failedEvent.getTimestamp());
    assertEquals("Signal timeout exceeded for key: approval", failedEvent.getErrorMessage());
    assertEquals(1, failedEvent.getRetryAttempt());
  }

  @Test
  void signalTimeoutRacePathDoesNotEscalateToDlqWhenScheduleRetryLoses() {
    handler =
        newHandler(
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
            Clock.systemUTC(),
            eventPublisher);
    JobEntity job = waitingJobWithMaxRetries(3);
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(false);

    handler.processSignalTimeout(job, now);

    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(eventPublisher, never()).publish(any());
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
    lenient().when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    return job;
  }

  private JobEntity waitingJob(UUID id, int maxRetries) {
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setMaxRetries(maxRetries);
    job.setStatus(JobStatus.WAITING);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setSignalKey("approval");
    job.setSignalTimeout(Instant.now().minusSeconds(1));
    job.setBackoffPolicy(BackoffPolicy.NONE);
    lenient().when(jobCrudStore.findById(id)).thenReturn(Optional.of(job));
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
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        txRegistry);
  }

  private JobTimeoutHandler newLeasedHandler(SingletonLeaseService singletonLeaseService) {
    return new JobTimeoutHandler(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        80,
        60L,
        Clock.systemUTC(),
        null,
        signalStore,
        metricsCollector,
        JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE,
        null,
        singletonLeaseService);
  }
}
