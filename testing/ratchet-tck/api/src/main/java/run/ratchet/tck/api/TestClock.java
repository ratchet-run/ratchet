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
package run.ratchet.tck.api;

import java.time.Duration;
import java.time.Instant;

/**
 * Deterministic time source for TCK contracts that exercise scheduling delays, retry backoffs, or
 * cron-driven recurring jobs without depending on wall-clock progression.
 *
 * <p>Implementations expose this only when their scheduler can be driven from a controllable clock.
 * Implementations whose executor is hardwired to wall-clock time MUST return {@link
 * java.util.Optional#empty()} from {@link RatchetTckRuntime#clock()}; contracts that require a
 * clock will then skip via {@code Assumptions.assumeTrue(...)} rather than fail.
 */
public interface TestClock {

  /** Current logical instant. Never null. */
  Instant now();

  /** Advances the logical clock by the given non-negative duration. */
  void advance(Duration delta);

  /** Sets the logical clock to the given instant. Must be at or after {@link #now()}. */
  void advanceTo(Instant target);
}
