package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobTimeoutHandlerTest {

  private static final UUID JOB_ID = new UUID(0L, 42L);
  private static final long TIMEOUT_SEC = 30L;

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private PostExecutionHandler lifecycleFacade;
  @Mock private MetricsCollector metricsCollector;

  private JobTimeoutHandler handler;

  @BeforeEach
  void setUp() {
    handler =
        new JobTimeoutHandler(
            jobCrudStore, jobRetryStore, jobBatchStatusStore, lifecycleFacade, 80, 60L);
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
    Instant now = Instant.now();
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobRetryStore.scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1)))
        .thenReturn(true);

    handler.processSignalTimeout(job, now);

    verify(jobRetryStore, times(1))
        .scheduleJobRetry(eq(JOB_ID), anyString(), any(Instant.class), eq(1));
    verify(jobBatchStatusStore, never()).compareAndSwapStatus(any(UUID.class), any(), any(), any());
    verify(lifecycleFacade, never()).handlePermanentFailure(any(), any());
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
    verify(lifecycleFacade, times(1)).handlePermanentFailure(eq(job), any());
  }

  @Test
  void signalTimeoutPublishesTimedOutMetricWhenFailureIsApplied() {
    JobTimeoutHandler metricsHandler =
        new JobTimeoutHandler(
            jobCrudStore,
            jobRetryStore,
            jobBatchStatusStore,
            lifecycleFacade,
            80,
            60L,
            Clock.systemUTC(),
            null,
            null,
            null,
            metricsCollector);
    JobEntity job = waitingJobWithMaxRetries(0);
    when(jobRetryStore.incrementRetryAttempt(JOB_ID)).thenReturn(1);
    when(jobBatchStatusStore.compareAndSwapStatus(
            eq(JOB_ID), eq(JobStatus.WAITING), eq(JobStatus.FAILED), anyString()))
        .thenReturn(true);

    metricsHandler.processSignalTimeout(job, Instant.now());

    verify(metricsCollector).signalTimedOut(JOB_ID, job.getPublicJobType(), "approval");
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
}
