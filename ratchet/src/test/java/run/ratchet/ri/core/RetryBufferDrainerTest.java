package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class RetryBufferDrainerTest {

  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<?> scheduledFuture;
  @Mock private RetryBufferManager retryBufferManager;
  @Mock private JobSubmissionService jobSubmissionService;
  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private DrainController drainController;

  @Test
  void scheduledDrainTask_suppressesDrainExceptions() {
    Runnable task = startAndCaptureTask();
    when(drainController.isDraining()).thenThrow(new RuntimeException("drain failed"));

    assertDoesNotThrow(task::run);
  }

  @Test
  void drain_submitException_requeuesPolledClaims() {
    RetryBufferManager.BufferedClaim first = bufferedClaim(1L);
    RetryBufferManager.BufferedClaim second = bufferedClaim(2L);
    Runnable task = startAndCaptureTask();

    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(2);
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, 2))
        .thenReturn(List.of(first, second));
    when(threadPoolManager.canAcceptWork(JobExecutionType.SINGLE)).thenReturn(true);
    doThrow(new RuntimeException("submit failed"))
        .when(jobSubmissionService)
        .submitBuffered(first.toClaimDto());

    assertDoesNotThrow(task::run);

    verify(retryBufferManager).forceOffer(first.toClaimDto());
    verify(retryBufferManager).forceOffer(second.toClaimDto());
  }

  @Test
  void drain_capacityDisappears_requeuesCurrentAndRemainingClaims() {
    RetryBufferManager.BufferedClaim first = bufferedClaim(1L);
    RetryBufferManager.BufferedClaim second = bufferedClaim(2L);
    Runnable task = startAndCaptureTask();

    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(2);
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, 2))
        .thenReturn(List.of(first, second));
    when(threadPoolManager.canAcceptWork(JobExecutionType.SINGLE)).thenReturn(false, true);

    assertDoesNotThrow(task::run);

    verify(retryBufferManager).forceOffer(first.toClaimDto());
    verify(retryBufferManager).forceOffer(second.toClaimDto());
    verify(jobSubmissionService, never()).submitBuffered(any(JobClaimDto.class));
  }

  private Runnable startAndCaptureTask() {
    ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doReturn(scheduledFuture)
        .when(scheduledExecutor)
        .scheduleAtFixedRate(taskCaptor.capture(), eq(1000L), eq(1000L), eq(TimeUnit.MILLISECONDS));

    RetryBufferDrainer drainer =
        new RetryBufferDrainer(
            executorProvider,
            retryBufferManager,
            jobSubmissionService,
            threadPoolManager,
            drainController,
            RatchetOptions.defaults());
    drainer.start();
    return taskCaptor.getValue();
  }

  private static RetryBufferManager.BufferedClaim bufferedClaim(long id) {
    return new RetryBufferManager.BufferedClaim(
        new UUID(0L, id),
        JobExecutionType.SINGLE,
        JobPriority.NORMAL,
        Instant.parse("2026-01-01T00:00:00Z").plusSeconds(id),
        30,
        "node-1",
        Instant.parse("2026-01-01T00:00:00Z"),
        null,
        0,
        3);
  }
}
