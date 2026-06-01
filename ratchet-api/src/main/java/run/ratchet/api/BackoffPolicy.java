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

/**
 * Retry backoff strategy applied between job execution attempts. Each policy uses the job's {@link
 * JobOptions#backoffParam} (a {@link java.time.Duration}) as its base delay.
 *
 * @since 0.1
 */
public enum BackoffPolicy {

  /** Retries immediately with no delay. */
  NONE,

  /**
   * Constant delay between retries equal to {@code backoffParam}.
   *
   * <p>Example (backoffParam = 5 s): immediate, +5 s, +5 s, +5 s, …
   */
  FIXED,

  /**
   * Delay doubles with each attempt, starting from {@code backoffParam}. Implementations apply an
   * upper bound on the computed delay; the reference implementation caps it at 24 hours.
   *
   * <p>Example (backoffParam = 1 s): immediate, +1 s, +2 s, +4 s, +8 s, …
   */
  EXPONENTIAL
}
