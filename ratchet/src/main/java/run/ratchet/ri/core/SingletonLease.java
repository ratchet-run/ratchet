package run.ratchet.ri.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.store.spi.LockStore;

/** Handle for an acquired cluster-wide singleton lease. */
public final class SingletonLease implements AutoCloseable {

  private static final Logger log = Logger.getLogger(SingletonLease.class);

  private final LockStore lockStore;
  private final String name;
  private final String ownerNode;
  private final AtomicBoolean closed = new AtomicBoolean();

  SingletonLease(LockStore lockStore, String name, String ownerNode) {
    this.lockStore = Objects.requireNonNull(lockStore, "lockStore must not be null");
    this.name = Objects.requireNonNull(name, "name must not be null");
    this.ownerNode = Objects.requireNonNull(ownerNode, "ownerNode must not be null");
  }

  public String name() {
    return name;
  }

  public String ownerNode() {
    return ownerNode;
  }

  public boolean renew(Duration extension) {
    SingletonLeaseService.requirePositiveDuration(extension, "extension");
    if (closed.get()) {
      return false;
    }
    return lockStore.renewLock(name, extension, ownerNode);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      lockStore.unlock(name, ownerNode);
    } catch (Exception e) {
      log.debugf(e, "Failed to release singleton lease %s for node %s", name, ownerNode);
    }
  }
}
