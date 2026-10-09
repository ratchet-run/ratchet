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
package run.ratchet.ri.security;

/** Shared UTF-8 size measurement for serialized payloads. */
public final class Utf8Length {
  private Utf8Length() {}

  /** Returns the byte length produced by Java's UTF-8 encoder without allocating a byte array. */
  public static long utf8Length(String value) {
    long bytes = 0;
    for (int i = 0; i < value.length(); i++) {
      char current = value.charAt(i);
      if (current <= 0x7f) {
        bytes++;
      } else if (current <= 0x7ff) {
        bytes += 2;
      } else if (Character.isHighSurrogate(current)
          && i + 1 < value.length()
          && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else if (Character.isSurrogate(current)) {
        // String#getBytes(UTF_8) replaces an unpaired UTF-16 surrogate with the one-byte '?'
        // replacement used by the JDK encoder.
        bytes++;
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }
}
