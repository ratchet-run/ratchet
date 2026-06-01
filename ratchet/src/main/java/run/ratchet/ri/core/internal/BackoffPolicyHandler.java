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

import run.ratchet.api.BackoffPolicy;

/**
 * Computes retry back-off delays. {@code attempts} is the <em>next</em> attempt number (1-based).
 * EXPONENTIAL delay = {@code baseMs * 2^(attempts-1)}, capped at 24 hours.
 *
 * @see BackoffPolicy
 */
final class BackoffPolicyHandler {

  /** 24 hours in milliseconds — upper bound for exponential delay. */
  private static final long MAX_EXPONENTIAL_DELAY_MS = 86_400_000L;

  /** 2^20 * 1000ms ≈ 17 min; caps the exponent to prevent long overflow. */
  private static final int MAX_EXPONENT = 20;

  private BackoffPolicyHandler() {
    /* util */
  }

  static long computeDelay(BackoffPolicy policy, int baseMs, int attempts) {
    return switch (policy) {
      case NONE -> 0L;
      case FIXED -> baseMs;
      case EXPONENTIAL -> {
        int cappedExponent = Math.min(attempts - 1, MAX_EXPONENT);
        long multiplier =
            1L << cappedExponent; // 2^cappedExponent using bit shift (no floating point)
        long exponentialDelay =
            (multiplier > 0 && baseMs <= MAX_EXPONENTIAL_DELAY_MS / multiplier)
                ? baseMs * multiplier
                : MAX_EXPONENTIAL_DELAY_MS;
        yield Math.min(exponentialDelay, MAX_EXPONENTIAL_DELAY_MS);
      }
    };
  }
}
