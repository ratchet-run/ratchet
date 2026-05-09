package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;

@ExtendWith(MockitoExtension.class)
class PollerTest {

  @Mock private JobClaimStore jobClaimStore;
  @Mock private JobExecutionCoordinator jobExecutionCoordinator;
  @Mock private NodeIdentityProvider nodeIdProvider;
  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private DrainController drainController;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;
  @Mock private CircuitBreakerRegistry circuitBreakerRegistry;

  private Poller poller;
  private CircuitBreaker claimCircuitBreaker;
  private RatchetOptions options;

  @BeforeEach
  void setUp() {
    options =
        RatchetOptions.builder()
            .polling(
                polling ->
                    polling
                        .batchSize(5)
                        .burstDelayMs(500L)
                        .minDelayMs(2000L)
                        .maxDelayMs(30000L)
                        .deepIdleDelayMs(60000L)
                        .deepIdleThresholdMs(300000L)
                        .idleThreshold(5))
            .build();
    lenient()
        .when(threadPoolManager.getThreadPoolHealth())
        .thenReturn(new EnumMap<>(JobExecutionType.class));
    lenient().when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(drainController.isDraining()).thenReturn(false);
    claimCircuitBreaker =
        new CircuitBreaker("store.claim", new CircuitBreakerConfiguration(50.0f, 2, 5_000L, 1, 2));
    when(circuitBreakerRegistry.getBreaker("store.claim", CircuitBreakerProfile.CLAIM_PATH))
        .thenReturn(claimCircuitBreaker);

    poller = newPoller(true);
    poller.init();
    clearInvocations(metricsCollector);
  }

  @Test
  void tick_drainModeSkipsClaimingAndReturnsCurrentDelay() {
    when(drainController.isDraining()).thenReturn(true);

    long nextDelay = poller.tick();

    verify(jobClaimStore, never()).claimNextBatchOptimized(any(), anyInt(), anyString(), any());
    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(threadPoolManager, never()).getAvailableCapacity(any());
    assertEquals(2000L, nextDelay);
  }

  @Test
  void tick_claimsPerExecutionTypeCapacity() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(2);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.BATCH_CHILD)).thenReturn(1);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.CHAIN_STEP)).thenReturn(0);
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.WORKFLOW_BRANCH)).thenReturn(0);

    JobClaimDto singleClaim = claim(1L, JobExecutionType.SINGLE, "single");
    JobClaimDto batchClaim = claim(2L, JobExecutionType.BATCH_CHILD, "batch");

    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE), eq(2), eq("node-1"), any()))
        .thenReturn(List.of(singleClaim));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.BATCH_CHILD), eq(1), eq("node-1"), any()))
        .thenReturn(List.of(batchClaim));

    long nextDelay = poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(eq(JobExecutionType.SINGLE), eq(2), eq("node-1"), any());
    verify(jobClaimStore)
        .claimNextBatchOptimized(eq(JobExecutionType.BATCH_CHILD), eq(1), eq("node-1"), any());
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(eq(JobExecutionType.CHAIN_STEP), anyInt(), anyString(), any());
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(
            eq(JobExecutionType.WORKFLOW_BRANCH), anyInt(), anyString(), any());
    verify(jobExecutionCoordinator).submit(singleClaim);
    verify(jobExecutionCoordinator).submit(batchClaim);
    verify(metricsCollector).jobsClaimed(JobExecutionType.SINGLE.name(), 1);
    verify(metricsCollector).jobsClaimed(JobExecutionType.BATCH_CHILD.name(), 1);
    verify(metricsCollector).pollerBreakerState("store.claim", "CLOSED");
    assertEquals(4000L, nextDelay);
  }

  @Test
  void tick_transientClaimFailureBacksOff() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(1);
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    long nextDelay = poller.tick();

    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(metricsCollector).claimTransientFailure(JobExecutionType.SINGLE.name());
    verify(metricsCollector, never()).jobsClaimed(anyString(), anyInt());
    verify(metricsCollector).pollerBreakerState("store.claim", "CLOSED");
    assertEquals(4000L, nextDelay);
  }

  @Test
  void tick_consecutiveTransientFailuresTripBreakerAndSkipSubsequentClaim() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(1);
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    poller.tick();
    poller.tick();
    long nextDelay = poller.tick();

    verify(jobClaimStore, times(2))
        .claimNextBatchOptimized(eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any());
    verify(metricsCollector, times(2)).pollerBreakerState("store.claim", "OPEN");
    assertEquals(CircuitBreaker.State.OPEN, claimCircuitBreaker.getState());
    assertTrue(nextDelay >= 5_000L);
    assertTrue(nextDelay <= options.polling().maxDelayMs());
  }

  @Test
  void tick_emptyClaimBatchUpdatesLoadAndDoesNotSubmitJobs() {
    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(1);
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any()))
        .thenReturn(List.of());

    long nextDelay = poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any());
    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(metricsCollector, never()).jobsClaimed(anyString(), anyInt());
    verify(threadPoolManager).getThreadPoolHealth();
    assertEquals(2000L, nextDelay);
  }

  @Test
  void tick_halfOpenProbeRecoversAfterOpenWait() {
    claimCircuitBreaker =
        new CircuitBreaker("store.claim", new CircuitBreakerConfiguration(50.0f, 2, 0L, 1, 2));
    when(circuitBreakerRegistry.getBreaker("store.claim", CircuitBreakerProfile.CLAIM_PATH))
        .thenReturn(claimCircuitBreaker);
    poller = newPoller(true);
    poller.init();

    when(threadPoolManager.getAvailableCapacity(JobExecutionType.SINGLE)).thenReturn(1);
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any()))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(List.of(claim(3L, JobExecutionType.SINGLE, "recovered")));

    poller.tick();
    poller.tick();
    long nextDelay = poller.tick();

    verify(jobClaimStore, times(3))
        .claimNextBatchOptimized(eq(JobExecutionType.SINGLE), eq(1), eq("node-1"), any());
    verify(jobExecutionCoordinator).submit(any(JobClaimDto.class));
    verify(metricsCollector).pollerBreakerState("store.claim", "HALF_OPEN");
    verify(metricsCollector, times(3)).pollerBreakerState("store.claim", "CLOSED");
    assertEquals(CircuitBreaker.State.CLOSED, claimCircuitBreaker.getState());
    assertEquals(4_000L, nextDelay);
  }

  private Poller newPoller(boolean breakerEnabled) {
    return new Poller(
        jobClaimStore,
        jobExecutionCoordinator,
        nodeIdProvider,
        threadPoolManager,
        drainController,
        pollerScheduler,
        options,
        metricsCollector,
        circuitBreakerRegistry,
        breakerEnabled,
        new DefaultPollingStrategyProvider(),
        () -> NodeTagFilter.NONE,
        5,
        null);
  }

  private JobClaimDto claim(long jobId, JobExecutionType jobType, String type) {
    return new JobClaimDto(
        new UUID(0L, jobId),
        JobStatus.RUNNING,
        jobType,
        JobPriority.NORMAL,
        Instant.now(),
        0,
        30,
        "node-1",
        Instant.now(),
        type,
        0,
        0);
  }
}
