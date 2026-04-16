package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PollerTest {

  @Mock private JobClaimStore jobClaimStore;
  @Mock private JobExecutionCoordinator jobExecutionCoordinator;
  @Mock private NodeIdentityProvider nodeIdProvider;
  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private DrainController drainController;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private RatchetConfiguration config;
  @Mock private MetricsCollector metricsCollector;

  private Poller poller;

  @BeforeEach
  void setUp() {
    when(config.getPollerBurstDelayMs()).thenReturn(500L);
    when(config.getPollerMinDelayMs()).thenReturn(2000L);
    when(config.getPollerMaxDelayMs()).thenReturn(30000L);
    when(config.getPollerDeepIdleDelayMs()).thenReturn(60000L);
    when(config.getPollerDeepIdleThresholdMs()).thenReturn(300000L);
    when(config.getPollerIdleThreshold()).thenReturn(5);
    when(threadPoolManager.getThreadPoolHealth()).thenReturn(new EnumMap<>(JobExecutionType.class));
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(drainController.isDraining()).thenReturn(false);

    poller =
        new Poller(
            jobClaimStore,
            jobExecutionCoordinator,
            nodeIdProvider,
            threadPoolManager,
            drainController,
            pollerScheduler,
            config,
            metricsCollector,
            new DefaultPollingStrategyProvider(),
            5);
    poller.init();
  }

  @Test
  void tick_claimsPerExecutionTypeCapacity() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(2);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.BATCH_CHILD)).thenReturn(1);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.CHAIN_STEP)).thenReturn(0);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.WORKFLOW_BRANCH)).thenReturn(0);

    JobClaimDto singleClaim =
        new JobClaimDto(
            1L,
            JobStatus.RUNNING,
            JobExecutionType.SINGLE,
            JobPriority.NORMAL,
            Instant.now(),
            0,
            30,
            "node-1",
            Instant.now(),
            "single",
            0,
            0);
    JobClaimDto batchClaim =
        new JobClaimDto(
            2L,
            JobStatus.RUNNING,
            JobExecutionType.BATCH_CHILD,
            JobPriority.NORMAL,
            Instant.now(),
            0,
            30,
            "node-1",
            Instant.now(),
            "batch",
            0,
            0);

    when(jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 2, "node-1"))
        .thenReturn(List.of(singleClaim));
    when(jobClaimStore.claimNextBatchOptimized(JobExecutionType.BATCH_CHILD, 1, "node-1"))
        .thenReturn(List.of(batchClaim));

    long nextDelay = poller.tick();

    verify(jobClaimStore).claimNextBatchOptimized(JobExecutionType.SINGLE, 2, "node-1");
    verify(jobClaimStore).claimNextBatchOptimized(JobExecutionType.BATCH_CHILD, 1, "node-1");
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(eq(JobExecutionType.CHAIN_STEP), anyInt(), anyString());
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(eq(JobExecutionType.WORKFLOW_BRANCH), anyInt(), anyString());
    verify(jobExecutionCoordinator).submit(singleClaim);
    verify(jobExecutionCoordinator).submit(batchClaim);
    verify(metricsCollector).jobsClaimed(JobExecutionType.SINGLE.name(), 1);
    verify(metricsCollector).jobsClaimed(JobExecutionType.BATCH_CHILD.name(), 1);
    assertEquals(4000L, nextDelay);
  }

  @Test
  void tick_transientClaimFailureBacksOff() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(1);
    when(jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 1, "node-1"))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    long nextDelay = poller.tick();

    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(metricsCollector).claimTransientFailure(JobExecutionType.SINGLE.name());
    verify(metricsCollector, never()).jobsClaimed(anyString(), anyInt());
    assertEquals(4000L, nextDelay);
  }
}
