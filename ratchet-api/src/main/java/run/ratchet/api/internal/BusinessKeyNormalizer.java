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
 * Normalizes business keys to the subset supported identically by every bundled store.
 *
 * <p>The ASCII restriction is intentional. Oracle may count a {@code VARCHAR2(255)} in bytes, while
 * SQL Server's indexed {@code VARCHAR} columns use the database collation's code page. Printable
 * ASCII is one byte and round-trips unchanged in both, so the 255-character limit has the same
 * meaning for every SQL store and for MongoDB's indexed key.
 */
public final class BusinessKeyNormalizer {

  public static final int MAX_LENGTH = 255;

  private static final char FIRST_PRINTABLE_ASCII = 0x20;
  private static final char LAST_PRINTABLE_ASCII = 0x7e;

  private BusinessKeyNormalizer() {}

  public static String normalize(String key) {
    if (key == null || key.isBlank()) {
      return null;
    }

    String normalized = key.trim();
    if (normalized.isEmpty()) {
      return null;
    }
    if (normalized.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Business key must be at most " + MAX_LENGTH + " characters, got " + normalized.length());
    }
    for (int i = 0; i < normalized.length(); i++) {
      char current = normalized.charAt(i);
      if (current < FIRST_PRINTABLE_ASCII || current > LAST_PRINTABLE_ASCII) {
        throw new IllegalArgumentException(
            "Business key must contain only printable ASCII characters (U+0020-U+007E)");
      }
    }
    return normalized;
  }
}
