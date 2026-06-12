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
package run.ratchet.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for job execution: priority, retry behavior, backoff strategy, and
 * timeout. Use {@link #defaults()} and the {@code with*} methods to build custom configurations.
 *
 * @param priority execution priority
 * @param maxRetries maximum number of retry attempts (0 = no retries)
 * @param backoffPolicy retry backoff strategy
 * @param backoffParam base delay or policy-specific backoff parameter
 * @param timeoutSec maximum execution time in seconds (0 = no timeout)
 * @see JobBuilder
 * @see BackoffPolicy
 */
public record JobOptions(
    JobPriority priority,
    int maxRetries,
    BackoffPolicy backoffPolicy,
    Duration backoffParam,
    int timeoutSec) {

  public JobOptions {
    Objects.requireNonNull(priority, "priority must not be null");
    Objects.requireNonNull(backoffPolicy, "backoffPolicy must not be null");
    Objects.requireNonNull(backoffParam, "backoffParam must not be null");
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0");
    }
    if (timeoutSec < 0) {
      throw new IllegalArgumentException("timeoutSec must be >= 0");
    }
  }

  /** Returns a JobOptions with NORMAL priority, no retries, no backoff, and no timeout. */
  public static JobOptions defaults() {
    return new JobOptions(JobPriority.NORMAL, 0, BackoffPolicy.NONE, Duration.ZERO, 0);
  }

  /** Returns a copy with the specified backoff policy and base delay. */
  public JobOptions withBackoff(BackoffPolicy bp, Duration param) {
    return new JobOptions(priority, maxRetries, bp, param, timeoutSec);
  }

  /** Returns a copy with the specified maximum retry count (must be &gt;= 0). */
  public JobOptions withMaxRetries(int r) {
    return new JobOptions(priority, r, backoffPolicy, backoffParam, timeoutSec);
  }

  /** Returns a copy with the specified execution priority. */
  public JobOptions withPriority(JobPriority p) {
    return new JobOptions(p, maxRetries, backoffPolicy, backoffParam, timeoutSec);
  }

  /**
   * Returns a copy with the specified execution timeout. The timeout is stored in whole seconds and
   * must fit in a signed 32-bit integer. Use {@link Duration#ZERO} to disable timeout enforcement;
   * a positive timeout must be at least 1 second.
   *
   * @throws NullPointerException if {@code t} is null
   * @throws IllegalArgumentException if {@code t} is negative, is positive but shorter than 1
   *     second (which would truncate to 0 and silently disable enforcement), or exceeds {@link
   *     Integer#MAX_VALUE} seconds
   */
  public JobOptions withTimeout(Duration t) {
    Objects.requireNonNull(t, "timeout must not be null");
    if (t.isNegative()) {
      throw new IllegalArgumentException("timeout must be >= 0 seconds");
    }
    long timeoutSeconds = t.toSeconds();
    if (timeoutSeconds == 0 && !t.isZero()) {
      throw new IllegalArgumentException(
          "timeout must be at least 1 second when positive; use Duration.ZERO to disable");
    }
    if (timeoutSeconds > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("timeout must be <= " + Integer.MAX_VALUE + " seconds");
    }
    return new JobOptions(priority, maxRetries, backoffPolicy, backoffParam, (int) timeoutSeconds);
  }
}
