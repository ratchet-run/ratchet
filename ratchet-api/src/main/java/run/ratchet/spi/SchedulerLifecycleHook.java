package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Optional CDI hook around Ratchet startup and shutdown. */
@Incubating
public interface SchedulerLifecycleHook {

  default void beforeStart() {}

  default void afterStart() {}

  default void beforeStop() {}

  default void afterStop() {}
}
