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

  private ExtensionValidation() {}

  public static void requireKey(String key) {
    if (key == null || key.isBlank()) {
      throw new IllegalArgumentException("property key must not be null or blank");
    }
  }

  public static void requireNamespace(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be null or blank");
    }
  }

  public static void requireState(String state) {
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
  }

  public static String escapeLike(String raw) {
    return raw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }
}
