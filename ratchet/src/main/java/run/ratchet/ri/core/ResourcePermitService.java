package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
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
public class ResourcePermitService {

  private static final Logger log = Logger.getLogger(ResourcePermitService.class);

  private final ResourcePermitStore resourcePermitStore;
  private final PollerScheduler pollerScheduler;

  protected ResourcePermitService() {
    this.resourcePermitStore = null;
    this.pollerScheduler = null;
  }

  @Inject
  public ResourcePermitService(
      ResourcePermitStore resourcePermitStore, PollerScheduler pollerScheduler) {
    this.resourcePermitStore = resourcePermitStore;
    this.pollerScheduler = pollerScheduler;
  }

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * @return true if permit was acquired, false if resource is at capacity
   */
  public boolean tryAcquire(String resourceName, UUID jobId, String nodeId) {
    boolean acquired = resourcePermitStore.tryAcquirePermit(resourceName, jobId, nodeId);
    if (acquired) {
      log.debugf("Job %s acquired permit for resource %s", jobId, resourceName);
    } else {
      log.debugf("Resource %s at capacity - job %s must wait", resourceName, jobId);
    }
    return acquired;
  }

  /** Safe to call even if the job holds no permit. */
  public void release(String resourceName, UUID jobId) {
    resourcePermitStore.releasePermit(resourceName, jobId);
    log.debugf("Job %s released permit for resource %s", jobId, resourceName);
    pollerScheduler.wakeup();
  }

  public void releaseAll(UUID jobId) {
    resourcePermitStore.releaseAllPermits(jobId);
    pollerScheduler.wakeup();
  }

  /**
   * @return delay in milliseconds, or 5000 as default if resource not found
   */
  public int getRetryDelay(String resourceName) {
    return resourcePermitStore.getPermitRetryDelay(resourceName);
  }

  public void configureResource(
      String resourceName, int maxConcurrent, int retryDelayMs, String description) {
    resourcePermitStore.configureResource(resourceName, maxConcurrent, retryDelayMs, description);
    log.infof(
        "Configured resource %s with max=%s, retryDelay=%sms",
        resourceName, maxConcurrent, retryDelayMs);
  }

  /** Call periodically, e.g. during node heartbeat. */
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds == null || staleNodeIds.isEmpty()) {
      return 0;
    }

    int deleted = resourcePermitStore.cleanupOrphanedPermits(staleNodeIds);

    if (deleted > 0) {
      log.infof("Cleaned up %s orphaned permit(s) from stale nodes", deleted);
    }

    return deleted;
  }
}
