package run.ratchet.store.spi;

import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  boolean tryAcquirePermit(String resource, UUID jobId, String nodeId);

  void releasePermit(String resource, UUID jobId);

  void releaseAllPermits(UUID jobId);

  int getPermitRetryDelay(String resource);

  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
