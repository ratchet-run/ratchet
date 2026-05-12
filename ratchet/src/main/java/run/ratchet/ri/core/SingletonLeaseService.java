package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.LockStore;

/** Acquires expiring cluster-wide leases for work that must run on at most one node at a time. */
@ApplicationScoped
public class SingletonLeaseService {

  private static final Logger log = Logger.getLogger(SingletonLeaseService.class);

  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  protected SingletonLeaseService() {
    this.lockStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public SingletonLeaseService(LockStore lockStore, NodeIdentityProvider nodeIdentityProvider) {
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  static String requireLeaseName(String leaseName) {
    String normalized = Objects.requireNonNull(leaseName, "leaseName must not be null").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("leaseName must not be blank");
    }
    return normalized;
  }

  static void requirePositiveDuration(Duration duration, String argumentName) {
    Objects.requireNonNull(duration, argumentName + " must not be null");
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(argumentName + " must be positive");
    }
  }

  public Optional<SingletonLease> tryAcquire(String leaseName, Duration ttl) {
    String normalizedName = requireLeaseName(leaseName);
    requirePositiveDuration(ttl, "ttl");

    String nodeId = nodeIdentityProvider.getNodeId();
    try {
      if (!lockStore.tryLock(normalizedName, ttl, nodeId)) {
        return Optional.empty();
      }
    } catch (RuntimeException e) {
      log.errorf(e, "Failed to acquire singleton lease %s for node %s", normalizedName, nodeId);
      return Optional.empty();
    }

    return Optional.of(new SingletonLease(lockStore, normalizedName, nodeId));
  }
}
