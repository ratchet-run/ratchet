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
import org.jboss.logging.Logger;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Handles submission failures by buffering for retry or resetting to PENDING. */
@ApplicationScoped
public class SubmissionFailureHandler {

  private static final Logger log = Logger.getLogger(SubmissionFailureHandler.class);

  private final JobStateManager jobStateManager;
  private final RetryBufferManager retryBufferManager;
  private final PoolRegistry poolRegistry;
  private final PollerScheduler pollerScheduler;
  private final MetricsCollector metricsCollector;

  protected SubmissionFailureHandler() {
    this.jobStateManager = null;
    this.retryBufferManager = null;
    this.poolRegistry = null;
    this.pollerScheduler = null;
    this.metricsCollector = null;
  }

  @Inject
  public SubmissionFailureHandler(
      JobStateManager jobStateManager,
      RetryBufferManager retryBufferManager,
      PoolRegistry poolRegistry,
      PollerScheduler pollerScheduler,
      MetricsCollector metricsCollector) {
    this.jobStateManager = jobStateManager;
    this.retryBufferManager = retryBufferManager;
    this.poolRegistry = poolRegistry;
    this.pollerScheduler = pollerScheduler;
    this.metricsCollector = metricsCollector;
  }

  void handleGateFailure(JobEntity job, GateCheckResult result, boolean isFirstAttempt) {
    recordGateRejected(job.getJobType(), result);
    if (isFirstAttempt) {
      ResetOutcome outcome = resetToPending(job);
      logFirstAttemptGateFailure(job, result, outcome);
    } else {
      if (!retryBufferManager.offer(job)) {
        if (resetToPending(job) == ResetOutcome.STALE_CLAIM) return;
        if (result.status() == GateCheckResult.GateStatus.NO_PERMITS) {
          log.warnf(
              "Buffer for %s is full - returning job %s to PENDING", job.getJobType(), job.getId());
        }
      }
    }
  }

  void handleGateFailure(JobClaimDto claim, GateCheckResult result) {
    recordGateRejected(claim.jobType(), result);
    if (bufferClaim(claim)) {
      log.info(result.reason());
      return;
    }
    jobStateManager.resetJobToPending(claim.id(), claim.claimSeq());
    log.info(result.reason());
  }

  public void handleRejection(
      JobEntity job, JobExecutionType jobType, String poolName, boolean isFirstAttempt) {

    if (isFirstAttempt) {
      if (resetToPending(job) == ResetOutcome.STALE_CLAIM) return;
      log.warnf("Executor for %s rejected job %s - returned to PENDING", jobType, job.getId());
    } else {
      if (retryBufferManager.offer(job)) {
        log.warnf("Executor for %s rejected buffered job %s - re-buffering", jobType, job.getId());
      } else {
        if (resetToPending(job) == ResetOutcome.STALE_CLAIM) return;
        log.warnf(
            "Buffer for %s is full - returning rejected job %s to PENDING", jobType, job.getId());
      }
    }
  }

  public void handleRejection(JobClaimDto claim, JobExecutionType jobType, String poolName) {

    if (bufferClaim(claim)) {
      log.warnf("Executor for %s rejected job %s - buffered locally", jobType, claim.id());
      return;
    }
    if (jobStateManager.resetJobToPending(claim.id(), claim.claimSeq())) {
      log.warnf("Executor for %s rejected job %s - returned to PENDING", jobType, claim.id());
      return;
    }
    // The reset handler already logged the stale claim; discard it.
  }

  public void handleUnexpectedException(
      JobEntity job,
      JobExecutionType jobType,
      String poolName,
      boolean isFirstAttempt,
      Exception exception) {
    log.errorf(
        exception,
        "Unexpected exception submitting job %s - retaining claim for retry",
        job.getId());

    if (isFirstAttempt || !retryBufferManager.offer(job)) {
      resetToPending(job);
    }
  }

  public void handleUnexpectedException(
      JobClaimDto claim, JobExecutionType jobType, String poolName, Exception exception) {
    log.errorf(
        exception,
        "Unexpected exception submitting job %s - retaining claim for retry",
        claim.id());

    if (bufferClaim(claim)) {
      return;
    }
    jobStateManager.resetJobToPending(claim.id(), claim.claimSeq());
  }

  void retainUnsubmittedClaim(JobClaimDto claim) {
    if (!retryBufferManager.forceOffer(claim)) {
      jobStateManager.resetJobToPending(claim.id(), claim.claimSeq());
    }
  }

  private ResetOutcome resetToPending(JobEntity job) {
    if (jobStateManager.resetJobToPending(job)) {
      return ResetOutcome.RESET_TO_PENDING;
    }
    return ResetOutcome.STALE_CLAIM;
  }

  private void logFirstAttemptGateFailure(
      JobEntity job, GateCheckResult result, ResetOutcome outcome) {
    if (outcome == ResetOutcome.RESET_TO_PENDING) {
      log.infof("%s - returned job %s to PENDING", result.reason(), job.getId());
    }
  }

  private void recordGateRejected(JobExecutionType jobType, GateCheckResult result) {
    if (metricsCollector != null && result != null && result.isBlocked()) {
      metricsCollector.gateRejected(jobType.name(), result.status().name());
    }
  }

  private boolean bufferClaim(JobClaimDto claim) {
    return retryBufferManager.offer(claim);
  }

  private enum ResetOutcome {
    RESET_TO_PENDING,
    STALE_CLAIM
  }
}
