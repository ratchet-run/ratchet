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

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import run.ratchet.api.JobFilter;

/** Normalizes application-facing job tags. Store SPI calls use the keys they receive. */
public final class TagNormalizer {
  private TagNormalizer() {}

  /**
   * Trims the tag and converts it to lowercase using {@link Locale#ROOT}.
   *
   * @param tag the application-facing tag, which may be null
   * @return the normalized tag, or null when the input is null
   */
  public static String normalize(String tag) {
    return tag == null ? null : tag.trim().toLowerCase(Locale.ROOT);
  }

  /**
   * Normalizes the tags of an application-facing filter before it reaches a store.
   *
   * @param filter the filter supplied by the application
   * @return the filter with normalized tags
   */
  public static JobFilter normalizeFilter(JobFilter filter) {
    Set<String> tags = filter.tags();
    if (tags == null || tags.isEmpty()) {
      return filter;
    }
    Set<String> normalized = new HashSet<>();
    for (String tag : tags) {
      normalized.add(normalize(tag));
    }
    return tags.equals(normalized) ? filter : filter.toBuilder().tags(normalized).build();
  }
}
