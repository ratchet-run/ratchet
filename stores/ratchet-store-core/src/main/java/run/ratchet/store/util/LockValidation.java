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
package run.ratchet.store.util;

import java.time.Duration;
import java.util.Objects;

/** Shared lock-argument validation and duration conversion used by store lock implementations. */
public final class LockValidation {

  private LockValidation() {}

  public static void requireLockName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must be non-empty");
    }
  }

  public static void requirePositiveDuration(Duration duration, String parameterName) {
    Objects.requireNonNull(duration, parameterName);
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(parameterName + " must be positive");
    }
  }

  public static long durationMicros(Duration duration) {
    long seconds = duration.getSeconds();
    long microsFromNanos = (duration.getNano() + 999L) / 1_000L;
    if (seconds > (Long.MAX_VALUE - microsFromNanos) / 1_000_000L) {
      return Long.MAX_VALUE;
    }
    return Math.max(1L, seconds * 1_000_000L + microsFromNanos);
  }
}
