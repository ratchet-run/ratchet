package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.time.Duration;

/**
 * Coordinates one-time or destructive startup actions across nodes.
 *
 * <p>The reference implementation uses the store's distributed lock/lease mechanism so startup
 * work can be safely gated without relying on an external leader-election system.
 */
@Incubating
public interface StartupCoordinator {

  /**
   * Attempts to acquire a startup lease for the named action.
   *
   * @param actionName logical name of the startup action
   * @param leaseTtl how long the lease should remain valid if acquired
   * @return {@code true} if this node acquired the lease and may proceed
   */
  boolean tryAcquire(String actionName, Duration leaseTtl);

  /**
   * Releases a startup lease previously acquired by this node.
   *
   * @param actionName logical name of the startup action
   */
  void release(String actionName);
}
