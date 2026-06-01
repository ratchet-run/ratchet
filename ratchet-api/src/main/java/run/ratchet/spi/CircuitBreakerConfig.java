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
 * Runtime configuration for one circuit breaker profile.
 *
 * @param failureRateThreshold percentage of failed calls, from {@code 0.0} to {@code 100.0}, that
 *     opens the breaker once {@code minimumCalls} has been reached
 * @param slidingWindowSize number of recent calls kept in the failure-rate window
 * @param waitDurationMs time in milliseconds an open breaker waits before moving to half-open
 * @param permittedCallsInHalfOpen number of trial calls allowed while half-open
 * @param minimumCalls minimum calls required before the failure-rate threshold can open the breaker
 */
@Incubating
public record CircuitBreakerConfig(
    float failureRateThreshold,
    int slidingWindowSize,
    long waitDurationMs,
    int permittedCallsInHalfOpen,
    int minimumCalls) {

  public CircuitBreakerConfig {
    if (!Float.isFinite(failureRateThreshold)
        || failureRateThreshold < 0.0f
        || failureRateThreshold > 100.0f) {
      throw new IllegalArgumentException(
          "failureRateThreshold must be between 0.0 and 100.0 inclusive");
    }
    if (slidingWindowSize <= 0) {
      throw new IllegalArgumentException("slidingWindowSize must be greater than 0");
    }
    if (waitDurationMs < 0) {
      throw new IllegalArgumentException("waitDurationMs must be greater than or equal to 0");
    }
    if (permittedCallsInHalfOpen <= 0) {
      throw new IllegalArgumentException("permittedCallsInHalfOpen must be greater than 0");
    }
    if (minimumCalls <= 0) {
      throw new IllegalArgumentException("minimumCalls must be greater than 0");
    }
  }
}
