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
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.RetryBufferManager.BufferedClaim;
import run.ratchet.ri.core.internal.ExecutionTargetClaimPlanner;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Periodically drains retry buffers by resubmitting jobs when executor capacity becomes available.
 * Runs on a configurable interval, defaulting to 1 second with a 50 ms floor, while respecting
 * priority ordering and drain mode.
 */
@ApplicationScoped
public class RetryBufferDrainer {

  private static final Logger log = Logger.getLogger(RetryBufferDrainer.class);

  private final AtomicBoolean started = new AtomicBoolean();
  private final Object lifecycleLock = new Object();

  private final ExecutorProvider executorProvider;
  private final RetryBufferManager retryBufferManager;
  private final JobSubmissionService jobSubmissionService;
  private final PoolRegistry poolRegistry;
  private final ExecutionTargetClaimPlanner claimPlanner;
  private final DrainController drainController;
  private final long drainIntervalMs;

  @SuppressWarnings("java:S3077")
  private volatile ScheduledFuture<?> drainerTask;

  protected RetryBufferDrainer() {
    this.executorProvider = null;
    this.retryBufferManager = null;
    this.jobSubmissionService = null;
    this.poolRegistry = null;
    this.claimPlanner = null;
    this.drainController = null;
    this.drainIntervalMs = 1000L;
  }

  @Inject
  RetryBufferDrainer(
      ExecutorProvider executorProvider,
      RetryBufferManager retryBufferManager,
      JobSubmissionService jobSubmissionService,
      PoolRegistry poolRegistry,
      DrainController drainController,
      RatchetOptions options) {
    this.executorProvider = executorProvider;
    this.retryBufferManager = retryBufferManager;
    this.jobSubmissionService = jobSubmissionService;
    this.poolRegistry = poolRegistry;
    this.claimPlanner = new ExecutionTargetClaimPlanner(poolRegistry, options);
    this.drainController = drainController;
    this.drainIntervalMs = Math.max(50L, options.retryBuffer().drainIntervalMs());
  }

  public void start() {
    synchronized (lifecycleLock) {
      if (!started.compareAndSet(false, true)) {
        return;
      }

      drainerTask =
          executorProvider
              .getScheduledExecutor()
              .scheduleAtFixedRate(
                  this::drainRetryBuffersSafely,
                  drainIntervalMs,
                  drainIntervalMs,
                  TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    synchronized (lifecycleLock) {
      started.set(false);
      ScheduledFuture<?> task = drainerTask;
      drainerTask = null;
      if (task != null && !task.isCancelled()) {
        task.cancel(false);
        log.info("RetryBufferDrainer shutdown complete");
      }
    }
  }

  private void drainRetryBuffersSafely() {
    try {
      drainRetryBuffers();
    } catch (Exception e) {
      log.error("Retry buffer drain failed", e);
    }
  }

  private void drainRetryBuffers() {
    if (drainController.isDraining()) {
      return;
    }

    for (JobExecutionType jobType : JobExecutionType.values()) {
      for (ExecutionTargetClaimPlanner.PoolClaimBudget budget : claimPlanner.budgets(jobType)) {
        drainRetryBufferForPool(jobType, budget);
      }
    }
  }

  private void drainRetryBufferForPool(
      JobExecutionType jobType, ExecutionTargetClaimPlanner.PoolClaimBudget budget) {
    while (!drainController.isDraining()) {
      int capacity = poolRegistry.availableCapacity(jobType, budget.poolName());
      if (capacity <= 0) {
        break;
      }

      List<BufferedClaim> bufferedJobs =
          retryBufferManager.pollBatchFromBuffer(jobType, budget.executionTargetFilter(), capacity);
      if (bufferedJobs.isEmpty()) {
        break;
      }

      boolean stopDrainingPool = false;
      for (int i = 0; i < bufferedJobs.size(); i++) {
        BufferedClaim buffered = bufferedJobs.get(i);
        if (drainController.isDraining()
            || !poolRegistry.canAcceptWork(jobType, budget.poolName())) {
          requeueRemaining(bufferedJobs, i);
          stopDrainingPool = true;
          break;
        }
        try {
          jobSubmissionService.submitBuffered(buffered.toClaimDto());
        } catch (Exception e) {
          requeueRemaining(bufferedJobs, i);
          throw e;
        }
      }
      if (stopDrainingPool) {
        break;
      }
    }
  }

  private void requeueRemaining(List<BufferedClaim> bufferedJobs, int fromIndex) {
    for (int i = fromIndex; i < bufferedJobs.size(); i++) {
      retryBufferManager.forceOffer(bufferedJobs.get(i).toClaimDto());
    }
  }
}
