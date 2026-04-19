package run.ratchet.ri.core;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubmissionFailureHandlerTest {

  @Mock private JobStateManager jobStateManager;
  @Mock private RetryBufferManager retryBufferManager;
  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;

  private SubmissionFailureHandler handler;

  @BeforeEach
  void setUp() {
    handler =
        new SubmissionFailureHandler(
            jobStateManager,
            retryBufferManager,
            threadPoolManager,
            pollerScheduler,
            metricsCollector);
  }

  @Test
  void handleGateFailure_jobRecordsGateMetric() {
    JobEntity job = new JobEntity();
    job.setId(42L);
    job.setJobType(JobExecutionType.SINGLE);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(job, GateCheckResult.noPermits(JobExecutionType.SINGLE, 42L), true);

    verify(metricsCollector).gateRejected(JobExecutionType.SINGLE.name(), "NO_PERMITS");
  }

  @Test
  void handleGateFailure_claimRecordsGateMetric() {
    JobClaimDto claim =
        new JobClaimDto(
            42L,
            JobStatus.RUNNING,
            JobExecutionType.BATCH_CHILD,
            null,
            null,
            0,
            30,
            "node-1",
            null,
            null,
            0,
            0);
    when(jobStateManager.resetJobToPending(claim.id())).thenReturn(true);

    handler.handleGateFailure(
        claim, GateCheckResult.rateLimited(JobExecutionType.BATCH_CHILD, 42L, 10, 5));

    verify(metricsCollector).gateRejected(JobExecutionType.BATCH_CHILD.name(), "RATE_LIMITED");
  }
}
