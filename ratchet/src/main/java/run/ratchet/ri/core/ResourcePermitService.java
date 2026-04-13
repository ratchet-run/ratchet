package run.ratchet.ri.core;

import run.ratchet.store.spi.ResourcePermitStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

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

  // Required by CDI proxy
  protected ResourcePermitService() {
    this.resourcePermitStore = null;
  }

  @Inject
  public ResourcePermitService(ResourcePermitStore resourcePermitStore) {
    this.resourcePermitStore = resourcePermitStore;
  }

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * @return true if permit was acquired, false if resource is at capacity
   * @throws IllegalArgumentException if resource is not configured
   */
  public boolean tryAcquire(String resourceName, long jobId, String nodeId) {
    boolean acquired = resourcePermitStore.tryAcquirePermit(resourceName, jobId, nodeId);
    if (acquired) {
      log.debugf("Job %s acquired permit for resource %s", jobId, resourceName);
    } else {
      log.debugf("Resource %s at capacity - job %s must wait", resourceName, jobId);
    }
    return acquired;
  }

  /** Safe to call even if the job holds no permit. */
  public void release(String resourceName, long jobId) {
    resourcePermitStore.releasePermit(resourceName, jobId);
    log.debugf("Job %s released permit for resource %s", jobId, resourceName);
  }

  public void releaseAll(long jobId) {
    resourcePermitStore.releaseAllPermits(jobId);
  }

  /**
   * @return delay in milliseconds, or 5000 as default if resource not found
   */
  public int getRetryDelay(String resourceName) {
    return resourcePermitStore.getPermitRetryDelay(resourceName);
  }

  /** Configures or updates a resource permit limit. */
  public void configureResource(
      String resourceName, int maxConcurrent, int retryDelayMs, String description) {
    resourcePermitStore.configureResource(resourceName, maxConcurrent, retryDelayMs, description);
    log.infof(
        "Configured resource %s with max=%s, retryDelay=%sms",
        resourceName, maxConcurrent, retryDelayMs);
  }

  /** Releases permits held by dead nodes. Call periodically, e.g. during node heartbeat. */
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
