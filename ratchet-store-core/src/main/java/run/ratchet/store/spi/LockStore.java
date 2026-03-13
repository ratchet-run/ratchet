package run.ratchet.store.spi;

import java.time.Duration;

/** Distributed lock operations for cluster-wide synchronization. */
public interface LockStore {

  /** Attempts to acquire a cluster-wide lock with the supplied TTL. */
  boolean tryLock(String name, Duration ttl, String nodeId);

  /** Releases a lock when it is still owned by the supplied node. */
  void unlock(String name, String nodeId);

  /** Extends the expiry of a lock owned by the supplied node. */
  boolean renewLock(String name, Duration extension, String nodeId);
}
