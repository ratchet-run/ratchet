package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.time.Duration;

/** Expiring store-backed lease operations for cluster-wide coordination. */
@Incubating
public interface LockStore {

  boolean tryLock(String name, Duration ttl, String nodeId);

  void unlock(String name, String nodeId);

  boolean renewLock(String name, Duration extension, String nodeId);
}
