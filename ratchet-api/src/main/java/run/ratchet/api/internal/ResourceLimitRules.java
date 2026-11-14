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

import java.time.Duration;
import run.ratchet.api.ResourceLimit;

/**
 * Normalizes resource limits to values supported identically by every store.
 *
 * <p>The limit key is {@code VARCHAR(100)}. Oracle counts bytes and SQL Server uses the collation's
 * code page, so printable ASCII makes the 100-character limit mean the same thing in every store.
 */
public final class ResourceLimitRules {

  public static final int MAX_NAME_LENGTH = 100;
  public static final int MAX_DESCRIPTION_LENGTH = 255;

  private static final Duration MAX_RETRY_DELAY = Duration.ofMillis(Integer.MAX_VALUE);

  private ResourceLimitRules() {}

  public static String normalizeName(String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    String normalized = name.trim();
    if (normalized.isEmpty()) {
      return null;
    }
    if (normalized.length() > MAX_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Resource name must be at most 100 characters, got " + normalized.length());
    }
    requirePrintableAscii(normalized, "Resource name");
    return normalized;
  }

  public static ResourceLimit normalize(ResourceLimit limit) {
    if (limit == null) {
      throw new IllegalArgumentException("Resource limit must not be null");
    }
    String name = normalizeName(limit.resourceName());
    if (name == null) {
      throw new IllegalArgumentException("Resource name must not be blank");
    }
    if (limit.maxConcurrent() < 1) {
      throw new IllegalArgumentException(
          "Resource limit maxConcurrent must be at least 1, got " + limit.maxConcurrent());
    }
    Duration retryDelay = limit.retryDelay();
    if (retryDelay.isNegative() || retryDelay.compareTo(MAX_RETRY_DELAY) > 0) {
      throw new IllegalArgumentException(
          "Resource limit retryDelay must be between 0 and 2147483647 ms, got " + retryDelay);
    }
    String description = limit.description();
    if (description != null) {
      description = description.trim();
      if (description.isEmpty()) {
        description = null;
      } else {
        if (description.length() > MAX_DESCRIPTION_LENGTH) {
          throw new IllegalArgumentException(
              "Resource description must be at most 255 characters, got " + description.length());
        }
        requirePrintableAscii(description, "Resource description");
      }
    }
    return new ResourceLimit(name, limit.maxConcurrent(), retryDelay, description);
  }

  private static void requirePrintableAscii(String value, String subject) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < 0x20 || c > 0x7e) {
        throw new IllegalArgumentException(
            subject + " must contain only printable ASCII characters (U+0020-U+007E)");
      }
    }
  }
}
