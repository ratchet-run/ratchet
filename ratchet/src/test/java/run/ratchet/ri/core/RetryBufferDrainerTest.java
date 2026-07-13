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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;

@ExtendWith(MockitoExtension.class)
class RetryBufferDrainerTest {

  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<?> scheduledFuture;
  @Mock private RetryBufferManager retryBufferManager;
  @Mock private JobSubmissionService jobSubmissionService;
  @Mock private PoolRegistry poolRegistry;
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
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(2));
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(2);
    when(retryBufferManager.pollBatchFromBuffer(
            eq(JobExecutionType.SINGLE), any(ExecutionTargetFilter.class), eq(2)))
        .thenReturn(List.of(first, second));
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(true);
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
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(platformCapacity(2));
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(2);
    when(retryBufferManager.pollBatchFromBuffer(
            eq(JobExecutionType.SINGLE), any(ExecutionTargetFilter.class), eq(2)))
        .thenReturn(List.of(first, second));
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(false, true);

    assertDoesNotThrow(task::run);

    verify(retryBufferManager).forceOffer(first.toClaimDto());
    verify(retryBufferManager).forceOffer(second.toClaimDto());
    verify(jobSubmissionService, never()).submitBuffered(any(JobClaimDto.class));
  }

  @Test
  void drain_mixedRoutingUsesEachPoolsCapacityAndFilter() {
    RetryBufferManager.BufferedClaim platformClaim = bufferedClaim(10L, ExecutorTargets.PLATFORM);
    RetryBufferManager.BufferedClaim virtualClaim = bufferedClaim(11L, ExecutorTargets.VIRTUAL);
    RetryBufferManager.BufferedClaim fallbackClaim = bufferedClaim(12L, "unknown");
    Runnable task = startAndCaptureTask();

    when(drainController.isDraining()).thenReturn(false);
    when(poolRegistry.hasPool(ExecutorTargets.PLATFORM)).thenReturn(true);
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(2, 2));
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(2, 0);
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(2, 0);
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.PLATFORM))
        .thenReturn(true);
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(true);
    ExecutionTargetFilter platformFilter =
        ExecutionTargetFilter.excluding(List.of(ExecutorTargets.VIRTUAL), true);
    ExecutionTargetFilter virtualFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), false);
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, platformFilter, 2))
        .thenReturn(List.of(platformClaim, fallbackClaim));
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, virtualFilter, 2))
        .thenReturn(List.of(virtualClaim));

    task.run();

    verify(jobSubmissionService).submitBuffered(platformClaim.toClaimDto());
    verify(jobSubmissionService).submitBuffered(fallbackClaim.toClaimDto());
    verify(jobSubmissionService).submitBuffered(virtualClaim.toClaimDto());
  }

  @Test
  void drain_singleTargetWorkloadPollsOnlyPoolWithCapacity() {
    RetryBufferManager.BufferedClaim virtualClaim = bufferedClaim(12L, ExecutorTargets.VIRTUAL);
    Runnable task = startAndCaptureTask();

    when(drainController.isDraining()).thenReturn(false);
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(0, 2));
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(2, 0);
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(true);
    ExecutionTargetFilter platformFilter =
        ExecutionTargetFilter.excluding(List.of(ExecutorTargets.VIRTUAL), true);
    ExecutionTargetFilter virtualFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), false);
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, virtualFilter, 2))
        .thenReturn(List.of(virtualClaim));

    task.run();

    verify(retryBufferManager, never())
        .pollBatchFromBuffer(eq(JobExecutionType.SINGLE), eq(platformFilter), anyInt());
    verify(jobSubmissionService).submitBuffered(virtualClaim.toClaimDto());
  }

  @Test
  void drain_nullTargetsUseDefaultPoolFilter() {
    RetryBufferManager.BufferedClaim defaultClaim = bufferedClaim(13L, null);
    Runnable task =
        startAndCaptureTask(
            RatchetOptions.builder()
                .execution(
                    execution ->
                        execution.defaultThreadingMode(RatchetOptions.ThreadingMode.VIRTUAL))
                .build());

    when(drainController.isDraining()).thenReturn(false);
    when(poolRegistry.hasPool(ExecutorTargets.VIRTUAL)).thenReturn(true);
    when(poolRegistry.availableCapacitiesByPool(JobExecutionType.SINGLE))
        .thenReturn(capacities(0, 2));
    when(poolRegistry.availableCapacity(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(2, 0);
    when(poolRegistry.canAcceptWork(JobExecutionType.SINGLE, ExecutorTargets.VIRTUAL))
        .thenReturn(true);
    ExecutionTargetFilter virtualDefaultFilter =
        ExecutionTargetFilter.matching(List.of(ExecutorTargets.VIRTUAL), true);
    when(retryBufferManager.pollBatchFromBuffer(JobExecutionType.SINGLE, virtualDefaultFilter, 2))
        .thenReturn(List.of(defaultClaim));

    task.run();

    verify(jobSubmissionService).submitBuffered(defaultClaim.toClaimDto());
  }

  @Test
  void shutdownDuringStart_cancelsScheduledTask() throws Exception {
    CountDownLatch scheduleEntered = new CountDownLatch(1);
    CountDownLatch releaseSchedule = new CountDownLatch(1);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doAnswer(
            invocation -> {
              scheduleEntered.countDown();
              assertTrue(releaseSchedule.await(5, TimeUnit.SECONDS));
              return scheduledFuture;
            })
        .when(scheduledExecutor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1000L), eq(1000L), eq(TimeUnit.MILLISECONDS));

    RetryBufferDrainer drainer =
        new RetryBufferDrainer(
            executorProvider,
            retryBufferManager,
            jobSubmissionService,
            poolRegistry,
            drainController,
            RatchetOptions.defaults());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> start = executor.submit(drainer::start);
      assertTrue(scheduleEntered.await(2, TimeUnit.SECONDS));
      Future<?> shutdown = executor.submit(drainer::shutdown);

      releaseSchedule.countDown();
      start.get(2, TimeUnit.SECONDS);
      shutdown.get(2, TimeUnit.SECONDS);

      verify(scheduledFuture).cancel(false);
    } finally {
      releaseSchedule.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private Runnable startAndCaptureTask() {
    return startAndCaptureTask(RatchetOptions.defaults());
  }

  private Runnable startAndCaptureTask(RatchetOptions options) {
    ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
    lenient().when(poolRegistry.availableCapacitiesByPool(any())).thenReturn(Map.of());
    lenient().when(poolRegistry.hasPool(ExecutorTargets.PLATFORM)).thenReturn(true);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doReturn(scheduledFuture)
        .when(scheduledExecutor)
        .scheduleAtFixedRate(taskCaptor.capture(), eq(1000L), eq(1000L), eq(TimeUnit.MILLISECONDS));

    RetryBufferDrainer drainer =
        new RetryBufferDrainer(
            executorProvider,
            retryBufferManager,
            jobSubmissionService,
            poolRegistry,
            drainController,
            options);
    drainer.start();
    return taskCaptor.getValue();
  }

  private static RetryBufferManager.BufferedClaim bufferedClaim(long id) {
    return bufferedClaim(id, null);
  }

  private static RetryBufferManager.BufferedClaim bufferedClaim(long id, String executionTarget) {
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
        3,
        executionTarget,
        null);
  }

  private static Map<String, Integer> platformCapacity(int platformCapacity) {
    return Map.of(ExecutorTargets.PLATFORM, platformCapacity);
  }

  private static Map<String, Integer> capacities(int platformCapacity, int virtualCapacity) {
    return Map.of(
        ExecutorTargets.PLATFORM, platformCapacity, ExecutorTargets.VIRTUAL, virtualCapacity);
  }
}
