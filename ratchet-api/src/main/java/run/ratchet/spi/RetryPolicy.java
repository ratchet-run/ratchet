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

import java.time.Duration;
import run.ratchet.api.Incubating;

/** Controls whether and when a failed job should be retried. */
@Incubating
public interface RetryPolicy {

  /**
   * Returns whether a failed job attempt should be retried.
   *
   * <p>The result is combined with the job's configured {@code maxRetries} using logical AND: the
   * engine schedules another attempt only when this method returns {@code true} <em>and</em> the
   * attempt is still within {@code maxRetries}. A policy MAY therefore terminate retries earlier
   * than {@code maxRetries}, but MUST NOT extend them beyond it — once {@code maxRetries} is
   * reached the job moves to terminal failure regardless of what this method returns. Use {@code
   * maxRetries} as the ceiling and this method to stop sooner.
   *
   * @param attempt 1-based attempt number
   * @param cause failure that ended the attempt; never {@code null}
   * @return {@code true} to schedule another attempt, {@code false} to move toward terminal failure
   */
  boolean shouldRetry(int attempt, Throwable cause);

  /**
   * Returns the retry delay for an attempt.
   *
   * @param attempt 1-based attempt number being scheduled
   * @return non-null delay. {@link Duration#ZERO} means retry immediately or fall back to the job's
   *     configured backoff policy, depending on the caller.
   * @apiNote Returning {@code null} violates the SPI contract and may fail the retry path with a
   *     {@link NullPointerException}.
   */
  Duration getDelay(int attempt);
}
