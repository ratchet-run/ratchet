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
package run.ratchet.ri.core.internal;

/**
 * Invocation slot for registering application-declared recurring jobs during runtime startup.
 *
 * <p>Invoking this callback is a scheduling hint, not a guarantee that recurring registration has
 * completed. Container integrations may defer or retry the actual registration work.
 */
@FunctionalInterface
public interface RecurringRegistration {

  /** Requests recurring-job registration for this runtime startup. */
  void register();

  /**
   * Prevents deferred recurring-registration work from running after this runtime begins shutting
   * down.
   *
   * <p>Implementations must make repeated calls safe. The default keeps simple, synchronous
   * registration callbacks source-compatible.
   */
  default void cancel() {}
}
