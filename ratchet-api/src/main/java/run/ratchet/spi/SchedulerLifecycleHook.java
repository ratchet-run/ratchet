package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Optional CDI hook around Ratchet startup and shutdown.
 *
 * <p>Hooks run in ascending {@code jakarta.annotation.Priority} order when a priority annotation is
 * present, then by implementation class name for deterministic tie-breaking. Unprioritized hooks
 * run after prioritized hooks.
 *
 * <p>Lifecycle phases are paired per hook. A hook receives {@link #afterStart()} only when its
 * {@link #beforeStart()} completed successfully, receives {@link #beforeStop()} only when it
 * completed startup, and receives {@link #afterStop()} only when its {@link #beforeStop()}
 * completed successfully.
 *
 * <p>Exceptions thrown from {@link #beforeStart()}, {@link #afterStart()}, {@link #beforeStop()},
 * or {@link #afterStop()} are logged at {@code WARN} and swallowed by the lifecycle observer so a
 * single misbehaving hook does not block deployment. The one exception is {@code
 * run.ratchet.store.migration.SchemaInitializationException} thrown from {@link #beforeStart()},
 * which propagates and aborts startup; an unmigrated or incompatible schema must not silently
 * become a runtime failure on the first store call.
 */
@Incubating
public interface SchedulerLifecycleHook {

  /**
   * Runs before Ratchet starts pollers, recurring registration, or maintenance work.
   *
   * <p>Throw {@code SchemaInitializationException} here to abort startup for an incompatible
   * schema.
   */
  default void beforeStart() {}

  /** Runs after Ratchet startup has completed. */
  default void afterStart() {}

  /** Runs before Ratchet begins shutdown. */
  default void beforeStop() {}

  /** Runs after Ratchet has requested scheduler shutdown. */
  default void afterStop() {}
}
