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
   * <p>SQL stores that implement this with row locks require the caller's store operation to run in
   * one active transaction so the capacity check and permit insert observe the same locked resource
   * row.
   *
   * @return {@code true} when a permit was acquired, {@code false} when the resource exists but is
   *     already at capacity
   * @throws IllegalArgumentException when {@code resource} has not been configured
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean tryAcquirePermit(String resource, UUID jobId, String nodeId);

  /**
   * Releases one permit. Missing permits are a no-op.
   *
   * @throws run.ratchet.api.exception.RatchetTransientStoreException if the backing store cannot
   *     complete the release
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void releasePermit(String resource, UUID jobId);

  /**
   * Releases all permits for one job. Unknown jobs are a no-op.
   *
   * @throws run.ratchet.api.exception.RatchetTransientStoreException if the backing store cannot
   *     complete the release
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  void releaseAllPermits(UUID jobId);

  /** Reads the retry delay for a resource. Transaction attribute: {@code SUPPORTS}. */
  int getPermitRetryDelay(String resource);

  /** Creates or updates a resource limit. Transaction attribute: {@code REQUIRED}. */
  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  /** Cleans permits owned by stale nodes. Transaction attribute: {@code REQUIRED}. */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
