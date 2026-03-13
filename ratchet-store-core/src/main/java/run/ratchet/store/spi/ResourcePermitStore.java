package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.util.List;

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  /** Attempts to acquire one permit for the supplied resource on behalf of a job. */
  boolean tryAcquirePermit(String resource, long jobId, String nodeId);

  /** Releases one permit previously acquired for a job. */
  void releasePermit(String resource, long jobId);

  /** Releases every permit row still associated with a job. */
  void releaseAllPermits(long jobId);

  /** Returns the configured retry delay, in milliseconds, for a constrained resource. */
  int getPermitRetryDelay(String resource);

  /** Creates or updates the concurrency configuration for a named resource. */
  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  /** Removes permits owned by stale nodes and returns the number cleaned up. */
  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
