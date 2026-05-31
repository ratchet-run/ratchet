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

import jakarta.enterprise.context.ApplicationScoped;
import run.ratchet.ri.core.internal.PollingStrategy;
import run.ratchet.spi.PollingConfig;
import run.ratchet.spi.PollingDelayStrategy;
import run.ratchet.spi.PollingStrategyProvider;

/** Default provider for the RI adaptive polling strategy. */
@ApplicationScoped
public class DefaultPollingStrategyProvider implements PollingStrategyProvider {

  @Override
  public PollingDelayStrategy create(PollingConfig config) {
    return new PollingStrategy(
        config.burstDelayMs(),
        config.minDelayMs(),
        config.maxDelayMs(),
        config.deepIdleDelayMs(),
        config.deepIdleThresholdMs(),
        config.idleThreshold(),
        config.batchSize());
  }
}
