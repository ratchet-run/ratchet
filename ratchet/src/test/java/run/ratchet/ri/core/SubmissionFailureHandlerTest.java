package run.ratchet.ri.core;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

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
    UUID jobId = new UUID(0L, 42L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(job, GateCheckResult.noPermits(JobExecutionType.SINGLE, jobId), true);

    verify(metricsCollector).gateRejected(JobExecutionType.SINGLE.name(), "NO_PERMITS");
  }

  @Test
  void handleGateFailure_claimRecordsGateMetric() {
    UUID claimJobId = new UUID(0L, 42L);
    JobClaimDto claim =
        new JobClaimDto(
            claimJobId,
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
        claim, GateCheckResult.rateLimited(JobExecutionType.BATCH_CHILD, claimJobId, 10, 5));

    verify(metricsCollector).gateRejected(JobExecutionType.BATCH_CHILD.name(), "RATE_LIMITED");
  }
}
