/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * single misbehaving hook does not block deployment. Store-provided schema initialization hooks may
 * throw a store-specific startup-abort exception from {@link #beforeStart()}, which propagates and
 * aborts startup; an unmigrated or incompatible schema must not silently become a runtime failure
 * on the first store call. API-only hook implementations should treat other exceptions as
 * best-effort warnings.
 */
@Incubating
public interface SchedulerLifecycleHook {

  /**
   * Runs before Ratchet starts pollers, recurring registration, or maintenance work.
   *
   * <p>Store modules may throw their startup-abort exception here to stop deployment for an
   * incompatible schema.
   */
  default void beforeStart() {}

  /** Runs after Ratchet startup has completed. */
  default void afterStart() {}

  /** Runs before Ratchet begins shutdown. */
  default void beforeStop() {}

  /** Runs after Ratchet has requested scheduler shutdown. */
  default void afterStop() {}
}
