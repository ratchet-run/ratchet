package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class DeadLetterServiceTest {

  @Mock private ExecutorProvider executorProvider;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private DlqAlertStore dlqAlertStore;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private ErrorSanitizer errorSanitizer;

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
            errorSanitizer);
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
    assertNotNull(alert.getAlertSentAt());
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

  private static JobEntity jobWithAttempts(int attempts) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setAttempts(attempts);
    return job;
  }
}
