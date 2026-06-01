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
