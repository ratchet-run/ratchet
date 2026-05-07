package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Optional CDI hook around Ratchet startup and shutdown.
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

  default void beforeStart() {}

  default void afterStart() {}

  default void beforeStop() {}

  default void afterStop() {}
}
