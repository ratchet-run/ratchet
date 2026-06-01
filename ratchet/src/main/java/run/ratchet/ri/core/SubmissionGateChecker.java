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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.ThreadPoolManager;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Pre-flight gate checker: drain → permit → rate-limit.
 *
 * <p>A {@code CLEAR} result means a thread-pool permit was acquired and must be handed to either
 * {@link JobExecutorService} for normal release on runner completion or {@link
 * SubmissionFailureHandler} for release on rejection/failure before execution starts.
 */
@ApplicationScoped
class SubmissionGateChecker {

  private final DrainController drainController;
  private final JobTypeRateLimiter rateLimiter;
  private final PoolRegistry poolRegistry;
  private final ExecutionTargetRouter router;

  protected SubmissionGateChecker() {
    this.drainController = null;
    this.rateLimiter = null;
    this.poolRegistry = null;
    this.router = null;
  }

  @Inject
  public SubmissionGateChecker(
      DrainController drainController,
      JobTypeRateLimiter rateLimiter,
      PoolRegistry poolRegistry,
      ExecutionTargetRouter router) {
    this.drainController = drainController;
    this.rateLimiter = rateLimiter;
    this.poolRegistry = poolRegistry;
    this.router = router;
  }

  /**
   * Checks all gates for the given job. On success, ownership of one permit transfers to caller,
   * acquired against the pool named on the returned {@link GateCheckResult#resolvedPoolName()}.
   *
   * <p>Retry submissions intentionally bypass drain mode because they already represent claimed
   * work owned by this node. The permit and rate-limit gates are separate best-effort checks; a
   * rate-limited job may hold a permit briefly before this method releases it.
   */
  public GateCheckResult check(JobEntity job, boolean isFirstAttempt) {
    return checkInternal(job.getJobType(), job.getId(), job.getExecutionTarget(), isFirstAttempt);
  }

  public GateCheckResult check(JobClaimDto claim, boolean isFirstAttempt) {
    return checkInternal(claim.jobType(), claim.id(), claim.executionTarget(), isFirstAttempt);
  }

  private GateCheckResult checkInternal(
      JobExecutionType jobType, UUID jobId, String executionTarget, boolean isFirstAttempt) {
    if (isFirstAttempt && drainController.isDraining()) {
      return GateCheckResult.draining(jobId);
    }

    // Resolve the effective pool exactly once, so permit acquire, release, and executor selection
    // all agree even when a virtual target falls back to platform.
    String poolName = router.resolve(executionTarget);
    ThreadPoolManager pool = poolRegistry.pool(poolName);

    if (!pool.tryAcquirePermit(jobType)) {
      return GateCheckResult.noPermits(jobType, jobId);
    }

    if (!rateLimiter.tryAcquire(jobType)) {
      pool.releasePermit(jobType);
      return GateCheckResult.rateLimited(
          jobType, jobId, rateLimiter.getCurrentCount(jobType), rateLimiter.getRateLimit(jobType));
    }

    return GateCheckResult.clear(poolName);
  }
}
