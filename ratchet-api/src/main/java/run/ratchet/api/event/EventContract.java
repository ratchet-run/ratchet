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
package run.ratchet.api.event;

import java.time.Duration;
import java.util.Objects;

final class EventContract {

  private EventContract() {}

  static <T> T requireNonNull(T value, String name) {
    return Objects.requireNonNull(value, name);
  }

  static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  static int requireNonNegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  static Duration requireNonNegative(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  static Long requireNonNegative(Long value, String name) {
    if (value != null && value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  static void requireBatchCounts(int totalItems, int completedItems, int failedItems) {
    requirePositive(totalItems, "totalItems");
    requireNonNegative(completedItems, "completedItems");
    requireNonNegative(failedItems, "failedItems");
    if ((long) completedItems + failedItems > totalItems) {
      throw new IllegalArgumentException("completedItems + failedItems must not exceed totalItems");
    }
  }
}
