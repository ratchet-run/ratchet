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
 * Initial settings used to create an adaptive polling delay strategy.
 *
 * @param burstDelayMs short delay used immediately after wakeups
 * @param minDelayMs minimum steady-state poll delay
 * @param maxDelayMs maximum backoff delay before deep idle
 * @param deepIdleDelayMs delay used after the poller has been idle long enough
 * @param deepIdleThresholdMs idle time required before entering deep idle
 * @param idleThreshold number of idle polls before increasing delay
 * @param batchSize target claim batch size used to interpret full vs partial polls
 * @apiNote Ratchet validates this configuration before constructing its built-in strategy: delays
 *     must be non-negative, {@code minDelayMs <= maxDelayMs <= deepIdleDelayMs}, {@code batchSize}
 *     must be positive, and {@code idleThreshold} must be non-negative.
 */
@Incubating
public record PollingConfig(
    long burstDelayMs,
    long minDelayMs,
    long maxDelayMs,
    long deepIdleDelayMs,
    long deepIdleThresholdMs,
    int idleThreshold,
    int batchSize) {

  public PollingConfig {
    if (burstDelayMs < 0) {
      throw new IllegalArgumentException("burstDelayMs must be non-negative");
    }
    if (minDelayMs < 0) {
      throw new IllegalArgumentException("minDelayMs must be non-negative");
    }
    if (maxDelayMs < 0) {
      throw new IllegalArgumentException("maxDelayMs must be non-negative");
    }
    if (deepIdleDelayMs < 0) {
      throw new IllegalArgumentException("deepIdleDelayMs must be non-negative");
    }
    if (deepIdleThresholdMs < 0) {
      throw new IllegalArgumentException("deepIdleThresholdMs must be non-negative");
    }
    if (minDelayMs > maxDelayMs) {
      throw new IllegalArgumentException("minDelayMs must be less than or equal to maxDelayMs");
    }
    if (maxDelayMs > deepIdleDelayMs) {
      throw new IllegalArgumentException(
          "maxDelayMs must be less than or equal to deepIdleDelayMs");
    }
    if (idleThreshold < 0) {
      throw new IllegalArgumentException("idleThreshold must be non-negative");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
  }
}
