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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;

class BulkRetryFiltersTest {

  @Test
  void normalizeIntersectsStatusesAndExcludesArchives() {
    JobFilter original =
        JobFilter.builder()
            .statuses(JobStatus.FAILED, JobStatus.PENDING)
            .tags("billing")
            .includeArchived(true)
            .build();

    JobFilter normalized = BulkRetryFilters.normalize(original, 25);

    assertEquals(Set.of(JobStatus.FAILED), normalized.statuses());
    assertEquals(Set.of("billing"), normalized.tags());
    assertFalse(normalized.includeArchived());
  }

  @Test
  void normalizeReturnsNoSelectionWhenStatusesExcludeFailed() {
    JobFilter pending = JobFilter.builder().statuses(JobStatus.PENDING).build();

    assertNull(BulkRetryFilters.normalize(pending, 25));
  }

  @Test
  void normalizeRejectsNullAndOutOfRangeLimits() {
    JobFilter filter = JobFilter.builder().build();

    assertThrows(NullPointerException.class, () -> BulkRetryFilters.normalize(null, 1));
    assertThrows(IllegalArgumentException.class, () -> BulkRetryFilters.normalize(filter, 0));
    assertThrows(IllegalArgumentException.class, () -> BulkRetryFilters.normalize(filter, 1001));
  }
}
