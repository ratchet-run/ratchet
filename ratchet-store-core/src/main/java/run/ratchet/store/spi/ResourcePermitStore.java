package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.util.List;

/** Resource permit management operations for concurrency limiting. */
@Incubating
public interface ResourcePermitStore {

  boolean tryAcquirePermit(String resource, long jobId, String nodeId);

  void releasePermit(String resource, long jobId);

  void releaseAllPermits(long jobId);

  int getPermitRetryDelay(String resource);

  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description);

  int cleanupOrphanedPermits(List<String> staleNodeIds);
}
