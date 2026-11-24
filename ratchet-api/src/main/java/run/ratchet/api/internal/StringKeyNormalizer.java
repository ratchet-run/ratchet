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

import run.ratchet.api.Nullable;

/** The whitespace rule shared by application and store keys. */
public final class StringKeyNormalizer {
  private StringKeyNormalizer() {}

  public static String required(@Nullable String key, String name) {
    if (key == null) throw new IllegalArgumentException(name + " must not be null or blank");
    String normalized = key.strip();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return normalized;
  }

  public static @Nullable String optional(@Nullable String key, String name) {
    return key == null ? null : required(key, name);
  }
}
