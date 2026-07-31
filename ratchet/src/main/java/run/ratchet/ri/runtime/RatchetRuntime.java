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
package run.ratchet.ri.runtime;

import java.time.Duration;

/**
 * Controls the lifecycle of a Ratchet runtime.
 *
 * <p>Both {@link #start()} and {@link #stop()} are idempotent.
 *
 * @apiNote This interface is incubating. Container integrations obtain the implementation from
 *     {@link RatchetRuntimeComponentCatalog}.
 */
public interface RatchetRuntime {

  /** Starts the runtime if it is not already running. */
  void start();

  /** Stops the runtime if it is running. */
  void stop();

  /**
   * Stops the runtime after allowing in-flight work up to {@code drainTimeout} to finish.
   *
   * <p>Container-neutral implementations that do not support bounded draining retain the no-arg
   * behavior.
   */
  default void stop(Duration drainTimeout) {
    stop();
  }
}
