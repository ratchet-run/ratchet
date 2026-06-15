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
package run.ratchet.coordinator.common;

/**
 * Shared validation guards for the bundled coordinator configuration records.
 *
 * <p>Each guard throws {@link IllegalArgumentException} with the exact message the individual
 * coordinator configs used before consolidation, so error wording and validation semantics are
 * unchanged. The four coordinator configs (Hazelcast, Infinispan, JMS, PostgreSQL) all shared the
 * same {@code > 0}, {@code >= 1}, and reconnect-backoff guard text verbatim.
 */
public final class CoordinatorConfigChecks {

  private CoordinatorConfigChecks() {}

  /**
   * Requires {@code value > 0}, throwing {@link IllegalArgumentException} with the message {@code
   * "<name> must be > 0"} otherwise.
   *
   * @param value the value to validate
   * @param name the field name used in the error message
   */
  public static void requirePositive(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
  }

  /**
   * Requires {@code value >= 1}, throwing {@link IllegalArgumentException} with the message {@code
   * "<name> must be >= 1"} otherwise.
   *
   * @param value the value to validate
   * @param name the field name used in the error message
   */
  public static void requireAtLeastOne(int value, String name) {
    if (value < 1) {
      throw new IllegalArgumentException(name + " must be >= 1");
    }
  }

  /**
   * Validates the reconnect backoff pair: {@code initialMs} must be {@code > 0} and {@code maxMs}
   * must be {@code >= initialMs}. Throws {@link IllegalArgumentException} with the original
   * messages ({@code "reconnectBackoffInitialMs must be > 0"} or {@code "reconnectBackoffMaxMs must
   * be >= reconnectBackoffInitialMs"}) otherwise.
   *
   * @param initialMs the initial reconnect delay in milliseconds
   * @param maxMs the cap on the doubled reconnect delay in milliseconds
   */
  public static void requireBackoffPair(long initialMs, long maxMs) {
    if (initialMs <= 0) {
      throw new IllegalArgumentException("reconnectBackoffInitialMs must be > 0");
    }
    if (maxMs < initialMs) {
      throw new IllegalArgumentException(
          "reconnectBackoffMaxMs must be >= reconnectBackoffInitialMs");
    }
  }
}
