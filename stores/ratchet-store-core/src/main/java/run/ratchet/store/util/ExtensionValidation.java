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

/** Shared extension-argument validation used by store extension implementations. */
public final class ExtensionValidation {

  /**
   * Maximum UTF-8 bytes of plaintext state, keeping state portable across stores. MySQL MEDIUMTEXT
   * and MongoDB documents stop at 16 MB, encryption adds about a third, and archiving copies every
   * namespace's state into one archived row or document.
   */
  public static final int MAX_STATE_BYTES = 1024 * 1024;

  private static final int MAX_KEY_LENGTH = 255;
  private static final int MAX_NAMESPACE_LENGTH = 64;
  private static final int MAX_VALUE_LENGTH = 1024;

  private ExtensionValidation() {}

  public static void requireKey(String key) {
    if (key == null || key.isBlank()) {
      throw new IllegalArgumentException("property key must not be null or blank");
    }
    if (key.length() > MAX_KEY_LENGTH) {
      throw new IllegalArgumentException(
          "property key must be at most " + MAX_KEY_LENGTH + " characters");
    }
  }

  public static void requireNamespace(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be null or blank");
    }
    if (namespace.length() > MAX_NAMESPACE_LENGTH) {
      throw new IllegalArgumentException(
          "namespace must be at most " + MAX_NAMESPACE_LENGTH + " characters");
    }
  }

  public static void requireValue(String value) {
    if (value != null && value.length() > MAX_VALUE_LENGTH) {
      throw new IllegalArgumentException(
          "property value must be at most " + MAX_VALUE_LENGTH + " characters");
    }
  }

  public static void requireState(String state) {
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    int bytes = 0;
    for (int i = 0; i < state.length(); i++) {
      char c = state.charAt(i);
      if (c <= 0x7f) {
        bytes++;
      } else if (c <= 0x7ff) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c)
          && i + 1 < state.length()
          && Character.isLowSurrogate(state.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else {
        bytes += Character.isSurrogate(c) ? 1 : 3;
      }
      if (bytes > MAX_STATE_BYTES) {
        throw new IllegalArgumentException(
            "state must be at most " + MAX_STATE_BYTES + " UTF-8 bytes");
      }
    }
  }

  public static String escapeLike(String raw) {
    return raw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }
}
