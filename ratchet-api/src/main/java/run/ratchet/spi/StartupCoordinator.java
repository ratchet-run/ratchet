package run.ratchet.spi;

import java.time.Duration;
import run.ratchet.api.Incubating;

/**
 * Coordinates one-time or destructive startup actions across nodes.
 *
 * <p>The reference implementation uses the store's distributed lock/lease mechanism so startup work
 * can be safely gated without relying on an external leader-election system.
 *
 * <p>Failed acquisitions are retryable. {@link #release(String)} must be safe when the caller does
 * not currently hold the lease, including when the lease has expired and another node acquired it.
 */
@Incubating
public interface StartupCoordinator {

  /**
   * Attempts to acquire a startup lease for the named action.
   *
   * @param actionName non-blank logical name of the startup action
   * @param leaseTtl how long the lease should remain valid if acquired; must be positive
   * @return {@code true} if this node acquired the lease and may proceed
   */
  boolean tryAcquire(String actionName, Duration leaseTtl);

  /**
   * Releases a startup lease previously acquired by this node.
   *
   * <p>Implementations must make this a no-op when the current node does not hold the named lease.
   *
   * @param actionName non-blank logical name of the startup action
   */
  void release(String actionName);
}
