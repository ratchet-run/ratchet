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
package run.ratchet.store.query;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobQuerySortField;

/**
 * Opaque keyset-pagination cursor encoding {@code (sortField, sortAscending, sortValue, jobId)}.
 *
 * <p>Encoded as URL-safe base64 with {@code |} delimiters so it can be passed in query parameters
 * without further escaping. Use {@link #encode()} to produce the opaque string for {@link
 * JobPage#nextCursor()} and {@link #decode(String)} to parse it back before building the store seek
 * predicate.
 *
 * <p>A cursor is only valid for the exact sort it was minted under. The store seek predicate
 * filters on {@link #sortField()} while the {@code ORDER BY} comes from the live filter, so a
 * caller that changes the sort field or direction mid-pagination would otherwise seek on one axis
 * while the query orders by another — silently skipping or repeating rows. {@link
 * #matchesFilterSort(JobFilter)} lets a store detect that mismatch and fall back to offset
 * pagination (a clean restart) instead.
 *
 * <p>Sort value encoding by field type:
 *
 * <ul>
 *   <li>{@code CREATED_AT, SCHEDULED_TIME, UPDATED_AT}: ISO-8601 {@link Instant} string
 *   <li>{@code PRIORITY}: decimal integer ordinal
 *   <li>{@code STATUS}: enum name string
 * </ul>
 */
public record JobQueryCursor(
    JobQuerySortField sortField, boolean sortAscending, String sortValue, UUID jobId) {

  public static JobQueryCursor decode(String cursor) {
    byte[] bytes;
    try {
      bytes = Base64.getUrlDecoder().decode(cursor);
    } catch (IllegalArgumentException e) {
      throw malformedCursor(e);
    }
    String raw = new String(bytes, StandardCharsets.UTF_8);
    int first = raw.indexOf('|');
    int second = raw.indexOf('|', first + 1);
    int third = raw.indexOf('|', second + 1);
    if (first < 0 || second < 0 || third < 0) {
      throw malformedCursor(null);
    }
    try {
      JobQuerySortField field = JobQuerySortField.valueOf(raw.substring(0, first));
      boolean ascending = parseAscending(raw.substring(first + 1, second));
      String sortValue = raw.substring(second + 1, third);
      UUID jobId = UUID.fromString(raw.substring(third + 1));
      return new JobQueryCursor(field, ascending, sortValue, jobId);
    } catch (IllegalArgumentException e) {
      throw malformedCursor(e);
    }
  }

  public String encode() {
    // The supported sort encodings reserve '|' as the field delimiter.
    String raw = sortField.name() + "|" + sortAscending + "|" + sortValue + "|" + jobId;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * True when this cursor matches the sort the given filter will order by. A keyset cursor is only
   * valid for the exact {@code (field, direction)} it was minted under; reusing one against a
   * different ordering would seek on one axis while the query sorts on another. An unset filter
   * sort field defaults to {@link JobQuerySortField#CREATED_AT}, matching the store {@code ORDER
   * BY} builders.
   */
  public boolean matchesFilterSort(JobFilter filter) {
    JobQuerySortField effective =
        filter.sortField() != null ? filter.sortField() : JobQuerySortField.CREATED_AT;
    return this.sortField == effective && this.sortAscending == filter.sortAscending();
  }

  private static boolean parseAscending(String token) {
    if ("true".equals(token)) {
      return true;
    }
    if ("false".equals(token)) {
      return false;
    }
    throw new IllegalArgumentException("Invalid cursor sort direction: " + token);
  }

  private static IllegalArgumentException malformedCursor(Throwable cause) {
    return cause == null
        ? new IllegalArgumentException("Malformed pagination cursor")
        : new IllegalArgumentException("Malformed pagination cursor", cause);
  }
}
