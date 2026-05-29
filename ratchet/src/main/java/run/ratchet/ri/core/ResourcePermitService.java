package run.ratchet.ri.core;

import java.util.List;
import java.util.UUID;

/**
 * SPI for the distributed semaphore that allows N concurrent job holders per named resource.
 * Default implementation: {@link run.ratchet.ri.core.internal.DefaultResourcePermitService}.
 *
 * @apiNote Framework SPI consumed by ri.core.JobTask / OrphanRecoveryTimer and by ratchet-testsuite
 *     integration tests. Applications must not implement this interface.
 */
public interface ResourcePermitService {

  /**
   * Attempts to acquire a permit atomically using pessimistic locking.
   *
   * @return true if permit was acquired, false if resource is at capacity
   */
  boolean tryAcquire(String resourceName, UUID jobId, String nodeId);

  /** Releases a permit held by a job. Safe to call even if the job holds no permit. */
  void release(String resourceName, UUID jobId);

  /** Releases all permits held by a job. */
  void releaseAll(UUID jobId);

  /**
   * @return delay in milliseconds, or 5000 as default if resource not found
   */
  int getRetryDelay(String resourceName);

  void configureResource(
      String resourceName, int maxConcurrent, int retryDelayMs, String description);

  /** Call periodically, e.g. during node heartbeat. */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
