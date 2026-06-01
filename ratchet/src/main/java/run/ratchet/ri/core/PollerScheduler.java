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
package run.ratchet.ri.core;

/**
 * SPI for the job-poller scheduler — controls poll-cycle lifecycle and wakeup signals. Default
 * implementation: {@link run.ratchet.ri.core.internal.DefaultPollerScheduler}.
 *
 * @apiNote Framework SPI consumed by ri.core collaborators (Poller, PollerWakeupListener,
 *     ResourcePermitService, PostExecutionHandler, etc.) and by ratchet-testsuite integration
 *     tests. Applications must not implement this interface.
 */
public interface PollerScheduler {

  void start();

  void stop();

  /**
   * Wakes up the poller to immediately check for available jobs. Called when a job notification is
   * received from the cluster, indicating that new work is available.
   */
  void wakeup();
}
