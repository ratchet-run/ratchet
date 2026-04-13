package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobTimeoutHandlerTest {

  private static final long JOB_ID = 42L;
  private static final long TIMEOUT_SEC = 30L;

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobStatusStore jobStatusStore;
  @Mock private PostExecutionHandler lifecycleFacade;

  private JobTimeoutHandler handler;

  @BeforeEach
  void setUp() {
    handler = new JobTimeoutHandler(jobCrudStore, jobStatusStore, lifecycleFacade, 80, 60L);
  }

  private JobEntity jobWithMaxRetries(int maxRetries) {
    JobEntity job = new JobEntity();
    job.setId(JOB_ID);
    job.setMaxRetries(maxRetries);
    job.setStatus(JobStatus.RUNNING);
    return job;
  }

  @Test
  void retriesRemainingReschedulesInsteadOfDlq() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobStatusStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobStatusStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobStatusStore, times(1))
        .scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1));
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobStatusStore, never()).compareAndSwapStatus(anyLong(), any(), any(), any());
  }

  @Test
  void retriesExhaustedCasesToFailedAndEscalatesDlq() {
    JobEntity job = jobWithMaxRetries(0);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobStatusStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobStatusStore, never()).scheduleJobRetry(anyLong(), anyString(), any(), anyInt());
    verify(jobStatusStore, times(1))
        .compareAndSwapStatus(eq(JOB_ID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), anyString());
    verify(lifecycleFacade, times(1)).handlePermanentFailure(eq(job), any());
  }

  @Test
  void racePathDoesNotEscalateToDlqWhenScheduleRetryLoses() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobStatusStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobStatusStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(false);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
    verify(jobStatusStore, never()).compareAndSwapStatus(anyLong(), any(), any(), any());
  }

  @Test
  void incrementRetryReturnsMinusOneExitsCleanly() {
    JobEntity job = jobWithMaxRetries(3);
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(jobStatusStore.incrementRetryAttempt(JOB_ID)).thenReturn(-1);

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobStatusStore, never()).scheduleJobRetry(anyLong(), anyString(), any(), anyInt());
    verify(jobStatusStore, never()).compareAndSwapStatus(anyLong(), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }

  @Test
  void missingJobExitsCleanly() {
    when(jobCrudStore.findById(JOB_ID)).thenReturn(Optional.empty());

    handler.processHardTimeout(JOB_ID, TIMEOUT_SEC);

    verify(jobStatusStore, never()).incrementRetryAttempt(anyLong());
    verify(jobStatusStore, never()).scheduleJobRetry(anyLong(), anyString(), any(), anyInt());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
  }
}
