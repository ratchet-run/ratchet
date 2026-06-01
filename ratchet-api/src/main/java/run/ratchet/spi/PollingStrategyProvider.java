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
 * Creates the polling delay strategy used by the RI poller.
 *
 * @since 0.1
 */
@Incubating
public interface PollingStrategyProvider {

  /**
   * Creates a polling delay strategy for one poller.
   *
   * @param config immutable initial polling settings; never {@code null}
   * @return new stateful polling delay strategy; never {@code null}
   */
  PollingDelayStrategy create(PollingConfig config);
}
