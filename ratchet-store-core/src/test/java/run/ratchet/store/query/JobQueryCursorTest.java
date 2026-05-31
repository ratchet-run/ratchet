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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQuerySortField;

class JobQueryCursorTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000465");

  @Test
  void decodeWrapsInvalidBase64AsMalformedCursor() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> JobQueryCursor.decode("%%%"));

    assertEquals("Malformed pagination cursor", ex.getMessage());
    assertInstanceOf(IllegalArgumentException.class, ex.getCause());
  }

  @Test
  void encodeDecodeRoundTripsEveryField() {
    JobQueryCursor original = new JobQueryCursor(JobQuerySortField.PRIORITY, true, "3", JOB_ID);

    JobQueryCursor decoded = JobQueryCursor.decode(original.encode());

    assertEquals(JobQuerySortField.PRIORITY, decoded.sortField());
    assertTrue(decoded.sortAscending(), "ascending flag must survive a round trip");
    assertEquals("3", decoded.sortValue());
    assertEquals(JOB_ID, decoded.jobId());
  }

  @Test
  void decodeRejectsCursorWithoutDirectionSegment() {
    // A legacy three-field cursor (field|value|jobId) is missing the direction segment.
    String legacy =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(
                ("CREATED_AT|2026-01-01T00:00:00Z|" + JOB_ID).getBytes(StandardCharsets.UTF_8));

    assertThrows(IllegalArgumentException.class, () -> JobQueryCursor.decode(legacy));
  }

  @Test
  void matchesFilterSortDetectsFieldChange() {
    JobQueryCursor cursor =
        new JobQueryCursor(JobQuerySortField.CREATED_AT, false, "2026-01-01T00:00:00Z", JOB_ID);

    assertTrue(cursor.matchesFilterSort(JobFilter.builder().build()));
    assertFalse(
        cursor.matchesFilterSort(JobFilter.builder().sortField(JobQuerySortField.PRIORITY).build()),
        "a cursor minted for CREATED_AT must not match a PRIORITY query");
  }

  @Test
  void matchesFilterSortDetectsDirectionChange() {
    JobQueryCursor cursor =
        new JobQueryCursor(JobQuerySortField.CREATED_AT, false, "2026-01-01T00:00:00Z", JOB_ID);

    assertFalse(
        cursor.matchesFilterSort(JobFilter.builder().sortAscending(true).build()),
        "a descending cursor must not match an ascending query");
  }
}
