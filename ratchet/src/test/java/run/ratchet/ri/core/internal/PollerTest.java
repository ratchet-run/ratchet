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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.DefaultPollingStrategyProvider;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PollingDelayStrategy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.spi.JobClaimStore;

@ExtendWith(MockitoExtension.class)
class PollerTest {

  @Mock private JobClaimStore jobClaimStore;
  @Mock private JobExecutionCoordinator jobExecutionCoordinator;
  @Mock private NodeIdentityProvider nodeIdProvider;
  @Mock private PoolRegistry poolRegistry;
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
        .when(poolRegistry.getThreadPoolHealth())
        .thenReturn(new EnumMap<>(JobExecutionType.class));
    lenient().when(poolRegistry.availableCapacitiesByPool(any())).thenReturn(Map.of());
    lenient().when(poolRegistry.hasPool(ExecutorTargets.PLATFORM)).thenReturn(true);
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

    verify(jobClaimStore, never())
        .claimNextBatchOptimized(
            any(JobExecutionType.class),
            anyInt(),
            anyString(),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(poolRegistry, never()).availableCapacitiesByPool(any());
    assertEquals(2000L, nextDelay);
  }

  @Test
  void tick_claimsPerExecutionTypeCapacity() {
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(2));
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.BATCH_CHILD))
        .thenReturn(platformCapacity(1));

    JobClaimDto singleClaim = claim(1L, JobExecutionType.SINGLE, "single");
    JobClaimDto batchClaim = claim(2L, JobExecutionType.BATCH_CHILD, "batch");

    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
        .thenReturn(List.of(singleClaim));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.BATCH_CHILD),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
        .thenReturn(List.of(batchClaim));

    long nextDelay = poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.BATCH_CHILD),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(
            eq(JobExecutionType.CHAIN_STEP),
            anyInt(),
            anyString(),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobClaimStore, never())
        .claimNextBatchOptimized(
            eq(JobExecutionType.WORKFLOW_BRANCH),
            anyInt(),
            anyString(),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobExecutionCoordinator).submit(singleClaim);
    verify(jobExecutionCoordinator).submit(batchClaim);
    verify(metricsCollector).jobsClaimed(JobExecutionType.SINGLE.name(), 1);
    verify(metricsCollector).jobsClaimed(JobExecutionType.BATCH_CHILD.name(), 1);
    verify(metricsCollector).pollerBreakerState("store.claim", "CLOSED");
    assertEquals(4000L, nextDelay);
  }

  @Test
  void tick_transientClaimFailureBacksOff() {
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(1));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
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
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(1));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    poller.tick();
    poller.tick();
    long nextDelay = poller.tick();

    verify(jobClaimStore, times(2))
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(metricsCollector, times(2)).pollerBreakerState("store.claim", "OPEN");
    assertEquals(CircuitBreaker.State.OPEN, claimCircuitBreaker.getState());
    assertTrue(nextDelay >= 5_000L);
    assertTrue(nextDelay <= options.polling().maxDelayMs());
  }

  @Test
  void tick_emptyClaimBatchUpdatesLoadAndDoesNotSubmitJobs() {
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(1));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
        .thenReturn(List.of());

    long nextDelay = poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
    verify(jobExecutionCoordinator, never()).submit(any(JobClaimDto.class));
    verify(metricsCollector, never()).jobsClaimed(anyString(), anyInt());
    verify(poolRegistry).getThreadPoolHealth();
    assertEquals(2000L, nextDelay);
  }

  @Test
  void tick_claimsPerPoolWhenTargetsAreMixed() {
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(2, 4));
    ExecutionTargetFilter platformFilter =
        ExecutionTargetFilter.excluding(List.of(ExecutorTargets.VIRTUAL), true);
    ExecutionTargetFilter virtualFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), false);
    JobClaimDto platformClaim =
        claim(10L, JobExecutionType.SINGLE, "platform", ExecutorTargets.PLATFORM);
    JobClaimDto virtualClaim =
        claim(11L, JobExecutionType.SINGLE, "virtual", ExecutorTargets.VIRTUAL);
    JobClaimDto fallbackClaim = claim(12L, JobExecutionType.SINGLE, "fallback", "unknown");

    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(platformFilter)))
        .thenReturn(List.of(platformClaim, fallbackClaim));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(4),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualFilter)))
        .thenReturn(List.of(virtualClaim));

    poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(platformFilter));
    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(4),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualFilter));
    verify(jobExecutionCoordinator).submit(platformClaim);
    verify(jobExecutionCoordinator).submit(fallbackClaim);
    verify(jobExecutionCoordinator).submit(virtualClaim);
  }

  @Test
  void tick_singleTargetWorkloadClaimsOnlyPoolWithCapacity() {
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(0, 3));
    ExecutionTargetFilter platformFilter =
        ExecutionTargetFilter.excluding(List.of(ExecutorTargets.VIRTUAL), true);
    ExecutionTargetFilter virtualFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), false);
    JobClaimDto virtualClaim =
        claim(12L, JobExecutionType.SINGLE, "virtual", ExecutorTargets.VIRTUAL);

    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(3),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualFilter)))
        .thenReturn(List.of(virtualClaim));

    poller.tick();

    verify(jobClaimStore, never())
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            anyInt(),
            anyString(),
            any(NodeTagFilter.class),
            eq(platformFilter));
    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(3),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualFilter));
    verify(jobExecutionCoordinator).submit(virtualClaim);
  }

  @Test
  void tick_nullTargetsClaimAgainstDefaultPool() {
    options = testOptions(RatchetOptions.ThreadingMode.VIRTUAL);
    poller = newPoller(true);
    poller.init();
    clearInvocations(metricsCollector);

    when(poolRegistry.hasPool(ExecutorTargets.VIRTUAL)).thenReturn(true);
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(0, 2));
    ExecutionTargetFilter virtualDefaultFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), true);
    JobClaimDto defaultClaim = claim(13L, JobExecutionType.SINGLE, "default", null);

    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualDefaultFilter)))
        .thenReturn(List.of(defaultClaim));

    poller.tick();

    verify(jobClaimStore)
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(2),
            eq("node-1"),
            any(NodeTagFilter.class),
            eq(virtualDefaultFilter));
    verify(jobExecutionCoordinator).submit(defaultClaim);
  }

  @Test
  void tick_usesInjectedClockForPollStartTime() {
    AtomicLong now = new AtomicLong(123_456L);
    RecordingDelayStrategy recordingStrategy = new RecordingDelayStrategy();
    poller =
        new Poller(
            jobClaimStore,
            jobExecutionCoordinator,
            nodeIdProvider,
            poolRegistry,
            drainController,
            pollerScheduler,
            options,
            metricsCollector,
            circuitBreakerRegistry,
            true,
            config -> recordingStrategy,
            () -> NodeTagFilter.NONE,
            5,
            null,
            now::get);
    poller.init();

    poller.tick();

    assertEquals(123_456L, recordingStrategy.pollStartTime);
  }

  @Test
  void tick_halfOpenProbeRecoversAfterOpenWait() {
    claimCircuitBreaker =
        new CircuitBreaker("store.claim", new CircuitBreakerConfiguration(50.0f, 2, 0L, 1, 2));
    when(circuitBreakerRegistry.getBreaker("store.claim", CircuitBreakerProfile.CLAIM_PATH))
        .thenReturn(claimCircuitBreaker);
    poller = newPoller(true);
    poller.init();

    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(1));
    when(jobClaimStore.claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(List.of(claim(3L, JobExecutionType.SINGLE, "recovered")));

    poller.tick();
    poller.tick();
    long nextDelay = poller.tick();

    verify(jobClaimStore, times(3))
        .claimNextBatchOptimized(
            eq(JobExecutionType.SINGLE),
            eq(1),
            eq("node-1"),
            any(NodeTagFilter.class),
            any(ExecutionTargetFilter.class));
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
        poolRegistry,
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

  private static RatchetOptions testOptions(RatchetOptions.ThreadingMode defaultThreadingMode) {
    return RatchetOptions.builder()
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
        .execution(execution -> execution.defaultThreadingMode(defaultThreadingMode))
        .build();
  }

  private static Map<String, Integer> platformCapacity(int platformCapacity) {
    return Map.of(ExecutorTargets.PLATFORM, platformCapacity);
  }

  private static Map<String, Integer> capacities(int platformCapacity, int virtualCapacity) {
    return Map.of(
        ExecutorTargets.PLATFORM, platformCapacity, ExecutorTargets.VIRTUAL, virtualCapacity);
  }

  private JobClaimDto claim(long jobId, JobExecutionType jobType, String type) {
    return claim(jobId, jobType, type, null);
  }

  private JobClaimDto claim(
      long jobId, JobExecutionType jobType, String type, String executionTarget) {
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
        0,
        executionTarget);
  }

  private static final class RecordingDelayStrategy implements PollingDelayStrategy {
    private long pollStartTime = Long.MIN_VALUE;

    @Override
    public long getCurrentDelay() {
      return 2000L;
    }

    @Override
    public void onWakeup() {}

    @Override
    public long recordPollResult(int jobCount, long pollStartTime) {
      this.pollStartTime = pollStartTime;
      return 2000L;
    }

    @Override
    public void updateSystemLoadFactor(double avgUtilization) {}

    @Override
    public boolean isInDeepIdle() {
      return false;
    }
  }
}
