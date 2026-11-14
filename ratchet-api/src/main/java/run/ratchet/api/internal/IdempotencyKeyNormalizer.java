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
package run.ratchet.api.internal;

/**
 * Normalizes idempotency keys to the UUID-sized subset supported identically by every bundled
 * store.
 *
 * <p>The ASCII restriction is intentional. Oracle may count a {@code VARCHAR2(36)} in bytes, while
 * SQL Server's {@code VARCHAR(36)} columns use the database collation's code page. Printable ASCII
 * is one byte and round-trips unchanged in both, so two distinct keys never collapse into one
 * stored value and the 36-character limit has the same meaning in every store.
 */
public final class IdempotencyKeyNormalizer {

  private static final char FIRST_PRINTABLE_ASCII = 0x20;
  private static final char LAST_PRINTABLE_ASCII = 0x7e;

  private IdempotencyKeyNormalizer() {}

  public static String normalize(String key) {
    if (key == null) {
      return null;
    }
    // trim() also strips control characters that isBlank() does not count as blank.
    String normalized = key.trim();
    if (normalized.isBlank()) {
      return null;
    }
    if (normalized.length() > 36) {
      throw new IllegalArgumentException(
          "Idempotency key must be at most 36 characters, got " + normalized.length());
    }
    for (int i = 0; i < normalized.length(); i++) {
      char current = normalized.charAt(i);
      if (current < FIRST_PRINTABLE_ASCII || current > LAST_PRINTABLE_ASCII) {
        throw new IllegalArgumentException(
            "Idempotency key must contain only printable ASCII characters (U+0020-U+007E)");
      }
    }
    return normalized;
  }
}
