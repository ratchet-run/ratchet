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
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.spi.ResourcePermitStore;

/**
 * Distributed semaphore allowing N concurrent job holders per named resource.
 *
 * <pre>{@code
 * if (resourcePermitService.tryAcquire(resourceName, jobId, nodeId)) {
 *     try { // execute job
 *     } finally { resourcePermitService.release(resourceName, jobId); }
 * } else { // reschedule }
 * }</pre>
 *
 * @see ResourcePermitStore
 */
@ApplicationScoped
public class DefaultResourcePermitService implements ResourcePermitService {

  private static final Logger log = Logger.getLogger(DefaultResourcePermitService.class);

  private final ResourcePermitStore resourcePermitStore;
  private final PollerScheduler pollerScheduler;

  protected DefaultResourcePermitService() {
    this.resourcePermitStore = null;
    this.pollerScheduler = null;
  }

  @Inject
  public DefaultResourcePermitService(
      Instance<ResourcePermitStore> resourcePermitStore, PollerScheduler pollerScheduler) {
    this.resourcePermitStore =
        resourcePermitStore.isResolvable() ? resourcePermitStore.get() : null;
    this.pollerScheduler = pollerScheduler;
    if (this.resourcePermitStore == null) {
      log.info(
          "ResourcePermitStore capability not advertised by the store — resource concurrency"
              + " gating is disabled (permits always granted)");
    }
  }

  /** Constructor for tests that supply a store directly (or {@code null} to disable gating). */
  public DefaultResourcePermitService(
      ResourcePermitStore resourcePermitStore, PollerScheduler pollerScheduler) {
    this.resourcePermitStore = resourcePermitStore;
    this.pollerScheduler = pollerScheduler;
  }

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * <p>TX attribute: REQUIRED. SQL stores depend on the caller's active transaction so the capacity
   * check and permit insert share the same locked resource row.
   *
   * @return true if permit was acquired, false if resource is at capacity
   */
  @Override
  public boolean tryAcquire(String resourceName, UUID jobId, String nodeId) {
    if (resourcePermitStore == null) {
      // No permit store: gating disabled, so every acquisition succeeds (unbounded concurrency).
      return true;
    }
    boolean acquired = resourcePermitStore.tryAcquirePermit(resourceName, jobId, nodeId);
    if (acquired) {
      log.debugf("Job %s acquired permit for resource %s", jobId, resourceName);
    } else {
      log.debugf("Resource %s at capacity - job %s must wait", resourceName, jobId);
    }
    return acquired;
  }

  /**
   * Releases a permit held by a job.
   *
   * <p>TX attribute: REQUIRED. Store implementations may depend on the caller's active transaction.
   * The poller wakeup still fires if the store release throws, so waiters are not stranded until
   * the next scheduled poll.
   *
   * <p>Safe to call even if the job holds no permit.
   */
  @Override
  public void release(String resourceName, UUID jobId) {
    try {
      if (resourcePermitStore != null) {
        resourcePermitStore.releasePermit(resourceName, jobId);
        log.debugf("Job %s released permit for resource %s", jobId, resourceName);
      }
    } finally {
      pollerScheduler.wakeup();
    }
  }

  /**
   * Releases all permits held by a job.
   *
   * <p>TX attribute: REQUIRED. Store implementations may depend on the caller's active transaction.
   * The poller wakeup still fires if the store release throws.
   */
  @Override
  public void releaseAll(UUID jobId) {
    try {
      if (resourcePermitStore != null) {
        resourcePermitStore.releaseAllPermits(jobId);
      }
    } finally {
      pollerScheduler.wakeup();
    }
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
      log.infof("Cleaned up %s orphaned permit(s) from stale nodes", deleted);
    }

    return deleted;
  }
}
