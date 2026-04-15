package run.ratchet.ri.core;

import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.LockStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;

/** Default {@link StartupCoordinator} backed by the store's distributed lock/lease mechanism. */
@ApplicationScoped
public class StoreBackedStartupCoordinator implements StartupCoordinator {

  private static final String LOCK_PREFIX = "startup:";

  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  protected StoreBackedStartupCoordinator() {
    this.lockStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public StoreBackedStartupCoordinator(
      LockStore lockStore, NodeIdentityProvider nodeIdentityProvider) {
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  @Override
  public boolean tryAcquire(String actionName, Duration leaseTtl) {
    return lockStore.tryLock(lockName(actionName), leaseTtl, nodeIdentityProvider.getNodeId());
  }

  @Override
  public void release(String actionName) {
    lockStore.unlock(lockName(actionName), nodeIdentityProvider.getNodeId());
  }

  private static String lockName(String actionName) {
    String normalized = Objects.requireNonNull(actionName, "actionName must not be null").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("actionName must not be blank");
    }
    return LOCK_PREFIX + normalized;
  }
}
