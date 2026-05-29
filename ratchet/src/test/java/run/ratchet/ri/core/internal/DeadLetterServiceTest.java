package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
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
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
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
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private DlqAlertStore dlqAlertStore;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private ErrorSanitizer errorSanitizer;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private LockStore lockStore;

  private DeadLetterService service;

  @BeforeEach
  void setUp() {
    service =
        new DeadLetterService(
            executorProvider,
            jobCrudStore,
            jobBulkStore,
            jobTerminalStore,
            singletonLeaseService,
            dlqAlertStore,
            eventPublisher,
            errorSanitizer,
            FIXED_CLOCK);
  }

  @Test
  void moveToDlq_savesAlertWhenNoRecentDuplicateExists() {
    JobEntity job = jobWithAttempts(2);
    RuntimeException cause = new RuntimeException("boom");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe error");
    when(dlqAlertStore.existsRecentDlqAlert(eq(job.getId()), anyString(), any(Instant.class)))
        .thenReturn(false);

    service.moveToDlq(job, cause);

    verify(jobTerminalStore).markJobFailedTerminal(job.getId(), "safe error", 2);
    assertEquals("safe error", job.getLastError());

    ArgumentCaptor<DlqAlertEntity> alertCaptor = ArgumentCaptor.forClass(DlqAlertEntity.class);
    verify(dlqAlertStore).saveDlqAlert(alertCaptor.capture());
    DlqAlertEntity alert = alertCaptor.getValue();
    assertEquals(job.getId(), alert.getJobId());
    assertEquals("system", alert.getAlertChannel());
    assertNotNull(alert.getErrorHash());
    assertEquals(FIXED_NOW, alert.getAlertSentAt());
    verify(dlqAlertStore)
        .existsRecentDlqAlert(
            eq(job.getId()), anyString(), eq(FIXED_NOW.minus(Duration.ofHours(1))));
  }

  @Test
  void moveToDlq_suppressesAlertWhenRecentDuplicateExists() {
    JobEntity job = jobWithAttempts(1);
    IllegalStateException cause = new IllegalStateException("duplicate");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe duplicate");
    when(dlqAlertStore.existsRecentDlqAlert(eq(job.getId()), anyString(), any(Instant.class)))
        .thenReturn(true);

    service.moveToDlq(job, cause);

    verify(jobTerminalStore).markJobFailedTerminal(job.getId(), "safe duplicate", 1);
    verify(dlqAlertStore, never()).saveDlqAlert(any());
  }

  @Test
  void moveToDlq_keepsTerminalTransitionWhenAlertRecordingFails() {
    JobEntity job = jobWithAttempts(3);
    RuntimeException cause = new RuntimeException("store down");
    when(errorSanitizer.sanitize(cause)).thenReturn("safe store down");
    when(dlqAlertStore.existsRecentDlqAlert(eq(job.getId()), anyString(), any(Instant.class)))
        .thenThrow(new IllegalStateException("alert store down"));

    assertDoesNotThrow(() -> service.moveToDlq(job, cause));

    verify(jobTerminalStore).markJobFailedTerminal(job.getId(), "safe store down", 3);
    assertEquals("safe store down", job.getLastError());
    verify(dlqAlertStore, never()).saveDlqAlert(any());
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
