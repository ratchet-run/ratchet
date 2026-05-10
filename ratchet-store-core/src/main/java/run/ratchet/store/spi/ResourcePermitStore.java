package run.ratchet.store.spi;

import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  /**
   * Attempts to acquire capacity for a configured resource.
   *
   * @return {@code true} when a permit was acquired, {@code false} when the resource exists but is
   *     already at capacity
   * @throws IllegalArgumentException when {@code resource} has not been configured
   */
  boolean tryAcquirePermit(String resource, UUID jobId, String nodeId);

  void releasePermit(String resource, UUID jobId);

  void releaseAllPermits(UUID jobId);

  int getPermitRetryDelay(String resource);

  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
