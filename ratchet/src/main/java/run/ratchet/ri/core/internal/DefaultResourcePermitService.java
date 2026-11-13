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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

/**
 * Distributed semaphore allowing N concurrent job holders per named resource.
 *
 * <pre>{@code
 * AcquireResult result = resourcePermitService.tryAcquire(resourceName, jobId, claimSeq, nodeId);
 * if (result == AcquireResult.ACQUIRED) {
 *     try { // execute job
 *     } finally { resourcePermitService.release(resourceName, jobId, claimSeq); }
 * } else { // store already parked the job, or the claim was superseded }
 * }</pre>
 *
 * @see ResourcePermitStore
 */
@ApplicationScoped
public class DefaultResourcePermitService implements ResourcePermitService {

  private static final Logger log = Logger.getLogger(DefaultResourcePermitService.class);

  private final ResourcePermitStore resourcePermitStore;
  // Bound background work so a large waiter population cannot monopolize a scan.
  static final int SWEEP_RESOURCE_LIMIT = 64;
  static final int STALE_DELETE_LIMIT = 500;

  private final JobWakeupService jobWakeupService;
  private final AfterCommitRegistrar afterCommitRegistrar;
  // Advance across sweeps so persistent waiters cannot starve later resource names.
  private volatile String waiterSweepCursor;

  protected DefaultResourcePermitService() {
    this.resourcePermitStore = null;
    this.jobWakeupService = null;
    this.afterCommitRegistrar = null;
  }

  @Inject
  public DefaultResourcePermitService(
      Instance<ResourcePermitStore> resourcePermitStore,
      JobWakeupService jobWakeupService,
      AfterCommitRegistrar afterCommitRegistrar) {
    this.resourcePermitStore =
        resourcePermitStore.isResolvable() ? resourcePermitStore.get() : null;
    this.jobWakeupService = jobWakeupService;
    this.afterCommitRegistrar = afterCommitRegistrar;
    if (this.resourcePermitStore == null) {
      log.info(
          "ResourcePermitStore capability not advertised by the store — resource concurrency"
              + " gating is disabled (permits always granted)");
    }
  }

  /** Constructor for tests that supply a store directly (or {@code null} to disable gating). */
  public DefaultResourcePermitService(
      ResourcePermitStore resourcePermitStore,
      JobWakeupService jobWakeupService,
      AfterCommitRegistrar afterCommitRegistrar) {
    this.resourcePermitStore = resourcePermitStore;
    this.jobWakeupService = jobWakeupService;
    this.afterCommitRegistrar = afterCommitRegistrar;
  }

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * <p>TX attribute: REQUIRED. The worker calls without an active transaction, so each attempt gets
   * its own short store transaction.
   *
   * @return ACQUIRED, AT_CAPACITY, or SUPERSEDED when the claim did not own the job at the check
   */
  @Override
  public AcquireResult tryAcquire(String resourceName, UUID jobId, long claimSeq, String nodeId) {
    if (resourcePermitStore == null) {
      // No permit store: gating disabled, so every acquisition succeeds (unbounded concurrency).
      return AcquireResult.ACQUIRED;
    }
    AcquireResult result =
        resourcePermitStore.tryAcquirePermit(resourceName, jobId, claimSeq, nodeId);
    log.debugf(
        "Permit result %s for job %s claim %s resource %s", result, jobId, claimSeq, resourceName);
    return result;
  }

  /**
   * Releases a permit held by the supplied claim, then wakes the resource's waiters once the
   * caller's transaction commits (at once when there is none). A failed wake-up is logged and left
   * to the waiter's own re-check and the periodic sweep.
   *
   * <p>Safe to call even if the job holds no permit.
   */
  @Override
  public void release(String resourceName, UUID jobId, long claimSeq) {
    if (resourcePermitStore == null) {
      return;
    }
    resourcePermitStore.releasePermit(resourceName, jobId, claimSeq);
    wakeAfterCommit(() -> wake(resourceName));
  }

  /**
   * Releases all permits held by a job, then runs the waiter sweep once the caller's transaction
   * commits, since the released resources are not known here.
   */
  @Override
  public void releaseAll(UUID jobId) {
    if (resourcePermitStore == null) {
      return;
    }
    resourcePermitStore.releaseAllPermits(jobId);
    wakeAfterCommit(this::sweepWaiters);
  }

  private void wakeAfterCommit(Runnable wake) {
    AfterCommitActions.runOrWarn(
        afterCommitRegistrar,
        "resource waiter wake",
        () -> {
          try {
            wake.run();
          } catch (RuntimeException e) {
            log.warn("Resource waiter wake failed; the periodic sweep will retry", e);
          }
        });
  }

  private void wake(String resource) {
    try {
      for (var waiter : resourcePermitStore.wakeWaiters(resource)) {
        jobWakeupService.notify(
            JobPriority.fromPersistedCode(waiter.priority()), true, waiter.executionTarget());
      }
    } catch (RuntimeException e) {
      log.warnf(
          e, "Could not wake waiters for resource %s; the periodic sweep will retry", resource);
    }
  }

  @Override
  public void sweepWaiters() {
    if (resourcePermitStore == null) {
      return;
    }
    List<String> resources =
        resourcePermitStore.findResourcesWithWaiters(waiterSweepCursor, SWEEP_RESOURCE_LIMIT);
    waiterSweepCursor =
        resources.size() < SWEEP_RESOURCE_LIMIT ? null : resources.get(resources.size() - 1);
    if (resources.isEmpty()) {
      return;
    }
    for (String resource : resources) {
      wake(resource);
    }
    resourcePermitStore.deleteStaleWaiters(STALE_DELETE_LIMIT);
  }

  /**
   * @return delay in milliseconds, or 5000 as default if resource not found
   */
  @Override
  public int getRetryDelay(String resourceName) {
    if (resourcePermitStore == null) {
      return 5000;
    }
    return resourcePermitStore.getPermitRetryDelay(resourceName);
  }

  @Override
  public void configureResource(
      String resourceName, int maxConcurrent, int retryDelayMs, String description) {
    if (resourcePermitStore == null) {
      return;
    }
    resourcePermitStore.configureResource(resourceName, maxConcurrent, retryDelayMs, description);
    wakeAfterCommit(() -> wake(resourceName));
    log.infof(
        "Configured resource %s with max=%s, retryDelay=%sms",
        resourceName, maxConcurrent, retryDelayMs);
  }

  /** Call periodically, e.g. during node heartbeat. */
  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (resourcePermitStore == null || staleNodeIds == null || staleNodeIds.isEmpty()) {
      return 0;
    }

    int deleted = resourcePermitStore.cleanupOrphanedPermits(staleNodeIds);

    if (deleted > 0) {
      wakeAfterCommit(this::sweepWaiters);
      log.infof("Cleaned up %s orphaned permit(s) from stale nodes", deleted);
    }

    return deleted;
  }
}
