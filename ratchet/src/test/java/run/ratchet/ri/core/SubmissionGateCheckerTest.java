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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.ThreadPoolManager;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class SubmissionGateCheckerTest {

  @Mock private DrainController drainController;
  @Mock private JobTypeRateLimiter rateLimiter;
  @Mock private PoolRegistry poolRegistry;
  @Mock private ExecutionTargetRouter router;
  @Mock private ThreadPoolManager pool;

  private SubmissionGateChecker gateChecker;

  private static JobEntity singleJob() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobClaimDto batchChildClaim() {
    return new JobClaimDto(
        UUID.randomUUID(),
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
        0,
        null,
        null);
  }

  private void routeToPlatform() {
    when(router.resolve(any())).thenReturn(ExecutorTargets.PLATFORM);
    when(poolRegistry.pool(ExecutorTargets.PLATFORM)).thenReturn(pool);
  }

  @BeforeEach
  void setUp() {
    gateChecker = new SubmissionGateChecker(drainController, rateLimiter, poolRegistry, router);
  }

  @Test
  void check_draining_firstAttempt_returnsDraining() {
    when(drainController.isDraining()).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.DRAINING, result.status());
    verify(pool, never()).tryAcquirePermit(any());
    verify(rateLimiter, never()).tryAcquire(any());
  }

  @Test
  void check_draining_notFirstAttempt_skipsDrainGate() {
    // isFirstAttempt=false short-circuits the drain condition before isDraining() is called,
    // so no isDraining stub is needed. Permit and rate checks still run normally.
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), false);

    assertTrue(result.isClear(), "retry should bypass the drain gate");
    assertEquals(ExecutorTargets.PLATFORM, result.resolvedPoolName());
    verify(drainController, never()).isDraining();
    verify(pool).tryAcquirePermit(JobExecutionType.SINGLE);
    verify(rateLimiter).tryAcquire(JobExecutionType.SINGLE);
  }

  @Test
  void check_noPermits_returnsNoPermits() {
    when(drainController.isDraining()).thenReturn(false);
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(false);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.NO_PERMITS, result.status());
    verify(rateLimiter, never()).tryAcquire(any());
  }

  @Test
  void check_rateLimited_releasesPermitBeforeReturning() {
    // This is the critical invariant: if rate-limited, the permit acquired from the resolved pool
    // must be released to prevent permanent Semaphore exhaustion.
    when(drainController.isDraining()).thenReturn(false);
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(false);
    when(rateLimiter.getCurrentCount(JobExecutionType.SINGLE)).thenReturn(5);
    when(rateLimiter.getRateLimit(JobExecutionType.SINGLE)).thenReturn(5);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.RATE_LIMITED, result.status());
    verify(pool).releasePermit(JobExecutionType.SINGLE);
  }

  @Test
  void check_allClear_returnsClear() {
    when(drainController.isDraining()).thenReturn(false);
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertTrue(result.isClear());
    assertEquals(ExecutorTargets.PLATFORM, result.resolvedPoolName());
    verify(pool, never()).releasePermit(any());
  }

  @Test
  void check_claimVariant_rateLimited_releasesPermit() {
    when(drainController.isDraining()).thenReturn(false);
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.BATCH_CHILD)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.BATCH_CHILD)).thenReturn(false);
    when(rateLimiter.getCurrentCount(JobExecutionType.BATCH_CHILD)).thenReturn(3);
    when(rateLimiter.getRateLimit(JobExecutionType.BATCH_CHILD)).thenReturn(3);

    JobClaimDto claim = batchChildClaim();
    GateCheckResult result = gateChecker.check(claim, true);

    assertEquals(GateCheckResult.GateStatus.RATE_LIMITED, result.status());
    verify(pool).releasePermit(JobExecutionType.BATCH_CHILD);
  }

  @Test
  void check_claimVariant_allClear_doesNotRelease() {
    when(drainController.isDraining()).thenReturn(false);
    routeToPlatform();
    when(pool.tryAcquirePermit(JobExecutionType.BATCH_CHILD)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.BATCH_CHILD)).thenReturn(true);

    GateCheckResult result = gateChecker.check(batchChildClaim(), true);

    assertTrue(result.isClear());
    verify(pool, never()).releasePermit(any());
  }
}
