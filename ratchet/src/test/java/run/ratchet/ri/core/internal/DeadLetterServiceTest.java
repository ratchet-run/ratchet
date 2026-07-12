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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.LockStore;

@ExtendWith(MockitoExtension.class)
class DeadLetterServiceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
  private static final String DAILY_CRON = "0 0 2 * * ?";

  @Mock private ExecutorProvider executorProvider;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private ErrorSanitizer errorSanitizer;
  @Mock private TransactionSynchronizationRegistry txRegistry;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private LockStore lockStore;

  private DeadLetterService service;

  @BeforeEach
  void setUp() {
    service =
        new DeadLetterService(
            executorProvider,
            jobBulkStore,
            jobTerminalStore,
            singletonLeaseService,
            eventPublisher,
            errorSanitizer,
            FIXED_CLOCK);
  }

  @Test
  void moveToDlqPersistsStateAndPublishesFailureThenDlqWithIdenticalMetadata() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), "safe error", 2)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    service.setTxRegistryForTesting(txRegistry);

    assertTrue(service.moveToDlq(job, cause));

    verify(jobTerminalStore).markJobFailedTerminal(job.getId(), "safe error", 2);
    assertEquals(JobStatus.FAILED, job.getStatus());
    assertEquals("safe error", job.getLastError());
    ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(events.capture());
    assertTrue(events.getAllValues().get(0) instanceof JobFailedEvent);
    assertTrue(events.getAllValues().get(1) instanceof JobDlqEvent);
    JobFailedEvent failed = (JobFailedEvent) events.getAllValues().get(0);
    JobDlqEvent dlq = (JobDlqEvent) events.getAllValues().get(1);
    assertEquals(job.getId(), failed.getJobId());
    assertEquals(job.getId(), dlq.getJobId());
    assertEquals("business-key", failed.getBusinessKey());
    assertEquals(failed.getBusinessKey(), dlq.getBusinessKey());
    assertEquals(JobPriority.HIGH, failed.getPriority());
    assertEquals(failed.getPriority(), dlq.getPriority());
    assertEquals("node-1", failed.getNodeId());
    assertEquals(failed.getNodeId(), dlq.getNodeId());
    assertEquals(FIXED_NOW, failed.getTimestamp());
    assertEquals(failed.getTimestamp(), dlq.getTimestamp());
    assertEquals("safe error", failed.getErrorMessage());
    assertEquals(failed.getErrorMessage(), dlq.getErrorMessage());
    assertEquals(2, failed.getRetryAttempt());
    assertEquals(failed.getRetryAttempt(), dlq.getRetryAttempt());
  }

  @Test
  void moveToDlqDoesNotPublishWhenTheTerminalTransitionLosesTheRace() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");

    assertFalse(service.moveToDlq(job, cause));

    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void moveToDlqPublishesBothTerminalEventsWhenNoRetryWasConsumed() {
    JobEntity job = jobWithAttempts(0);
    RuntimeException cause = new RuntimeException("retry buffer hard cap");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe overflow");
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), "safe overflow", 0)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    service.setTxRegistryForTesting(txRegistry);

    assertTrue(service.moveToDlq(job, cause));

    ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(events.capture());
    JobFailedEvent failed = (JobFailedEvent) events.getAllValues().get(0);
    JobDlqEvent dlq = (JobDlqEvent) events.getAllValues().get(1);
    assertEquals(0, failed.getRetryAttempt());
    assertEquals(0, dlq.getRetryAttempt());
  }

  @Test
  void recordDlqTransitionPublishesWithoutRepeatingTheTerminalMutation() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    service.setTxRegistryForTesting(txRegistry);

    service.recordDlqTransition(job, cause);

    verify(jobTerminalStore, never()).markJobFailedTerminal(eq(job.getId()), any(), anyInt());
    verify(eventPublisher).publish(any(JobDlqEvent.class));
    verify(eventPublisher, never()).publish(any(JobFailedEvent.class));
  }

  @Test
  void recordDlqTransitionPublishesTheExactPersistedTerminalError() {
    JobEntity job = jobWithAttempts(2);
    job.setLastError("exact persisted terminal error");
    RuntimeException cause = new RuntimeException("raw secret");
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    service.setTxRegistryForTesting(txRegistry);

    service.recordDlqTransition(job, cause);

    verify(errorSanitizer, never()).sanitize(any());
    ArgumentCaptor<JobDlqEvent> event = ArgumentCaptor.forClass(JobDlqEvent.class);
    verify(eventPublisher).publish(event.capture());
    assertEquals("exact persisted terminal error", event.getValue().getErrorMessage());
  }

  @Test
  void moveToDlqFallsBackSafelyWhenCustomSanitizerThrows() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("raw secret");
    String fallback = RuntimeException.class.getName();
    when(errorSanitizer.sanitize(cause)).thenThrow(new AssertionError("broken sanitizer"));
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), fallback, 2)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    service.setTxRegistryForTesting(txRegistry);

    assertTrue(service.moveToDlq(job, cause));

    assertEquals(fallback, job.getLastError());
    ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(events.capture());
    JobFailedEvent failed = (JobFailedEvent) events.getAllValues().get(0);
    JobDlqEvent dlq = (JobDlqEvent) events.getAllValues().get(1);
    assertEquals(fallback, failed.getErrorMessage());
    assertEquals(fallback, dlq.getErrorMessage());
  }

  @Test
  void recordDlqTransitionUsesAnIndependentTransaction() throws NoSuchMethodException {
    Transactional transactional =
        DeadLetterService.class
            .getMethod("recordDlqTransition", JobEntity.class, Throwable.class)
            .getAnnotation(Transactional.class);

    assertNotNull(transactional);
    assertEquals(Transactional.TxType.REQUIRES_NEW, transactional.value());
  }

  @Test
  void moveToDlqPublishesOnlyAfterTheTransitionTransactionCommits() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), "safe error", 2)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    service.setTxRegistryForTesting(txRegistry);
    ArgumentCaptor<Synchronization> synchronization =
        ArgumentCaptor.forClass(Synchronization.class);

    assertTrue(service.moveToDlq(job, cause));

    verify(txRegistry).registerInterposedSynchronization(synchronization.capture());
    verify(eventPublisher, never()).publish(any());

    synchronization.getValue().afterCompletion(Status.STATUS_COMMITTED);

    ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(2)).publish(events.capture());
    assertTrue(events.getAllValues().get(0) instanceof JobFailedEvent);
    assertTrue(events.getAllValues().get(1) instanceof JobDlqEvent);
  }

  @Test
  void moveToDlqSuppressesTheEventWhenTheTransitionTransactionRollsBack() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), "safe error", 2)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    service.setTxRegistryForTesting(txRegistry);
    ArgumentCaptor<Synchronization> synchronization =
        ArgumentCaptor.forClass(Synchronization.class);

    assertTrue(service.moveToDlq(job, cause));
    verify(txRegistry).registerInterposedSynchronization(synchronization.capture());

    synchronization.getValue().afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void moveToDlqSuppressesEventsWhenAfterCommitRegistrationFails() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(jobTerminalStore.markJobFailedTerminal(job.getId(), "safe error", 2)).thenReturn(true);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doThrow(new IllegalStateException("registration failed"))
        .when(txRegistry)
        .registerInterposedSynchronization(any(Synchronization.class));
    service.setTxRegistryForTesting(txRegistry);

    assertTrue(service.moveToDlq(job, cause));

    verify(eventPublisher, never()).publish(any());
  }

  @Test
  void purgeUsesFixedClockCutoff() {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    service.init(7, parsedCron());
    when(singletonLeaseService.tryAcquire(eq("dlqPurger"), any(Duration.class)))
        .thenReturn(acquiredLease());

    service.purge();

    verify(jobBulkStore).deleteDlqOlderThan(FIXED_NOW.minus(Duration.ofDays(7)));
  }

  @Test
  void initSchedulesNextExecutionFromFixedClock() {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    Cron cron = parsedCron();

    service.init(7, cron);

    Instant next =
        ExecutionTime.forCron(cron)
            .nextExecution(FIXED_NOW.atZone(ZoneId.systemDefault()))
            .map(ZonedDateTime::toInstant)
            .orElseThrow();
    verify(scheduledExecutor)
        .schedule(
            any(Runnable.class),
            eq(Duration.between(FIXED_NOW, next).toMillis()),
            eq(TimeUnit.MILLISECONDS));
  }

  @Test
  void runDoesNotPropagateWhenRescheduleIsRejectedDuringShutdown() {
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    when(singletonLeaseService.tryAcquire(eq("dlqPurger"), any(Duration.class)))
        .thenReturn(Optional.empty());
    service.init(7, parsedCron());

    reset(scheduledExecutor);
    when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new RejectedExecutionException("executor stopping"));

    assertDoesNotThrow(service::run);
  }

  private static JobEntity jobWithAttempts(int attempts) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setBusinessKey("business-key");
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.HIGH);
    job.setPickedBy("node-1");
    job.setAttempts(attempts);
    return job;
  }

  private static Cron parsedCron() {
    return CRON_PARSER.parse(DAILY_CRON);
  }

  private Optional<SingletonLease> acquiredLease() {
    return Optional.of(new SingletonLease(lockStore, "dlqPurger", "node-1"));
  }
}
