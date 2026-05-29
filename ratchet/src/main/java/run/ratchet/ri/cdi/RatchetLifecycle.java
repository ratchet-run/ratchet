package run.ratchet.ri.cdi;

/**
 * CDI lifecycle SPI for the Ratchet job scheduler — exposes {@code onShutdown()} so deployments can
 * invoke an orderly stop (and so integration tests can verify the shutdown ordering reflectively).
 * Default implementation: {@link run.ratchet.ri.cdi.internal.DefaultRatchetLifecycle}.
 *
 * @apiNote Startup is wired automatically via CDI observers on the default implementation;
 *     applications do not typically call into this interface. The shutdown method is exposed so
 *     ratchet-testsuite integration tests can drive teardown without tearing down the CDI
 *     container.
 */
public interface RatchetLifecycle {

  /**
   * Stops the scheduler subsystem: drains, stops timers, fires {@code beforeStop}/{@code afterStop}
   * lifecycle hooks, and releases dependent hook instances. Idempotent.
   */
  void onShutdown();
}
