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

import java.util.Objects;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;

/** Normalizes public query filters for bounded recovery of FAILED jobs. */
public final class BulkRetryFilters {

  public static final int MAX_LIMIT = 1000;

  private BulkRetryFilters() {}

  /**
   * Returns the live FAILED-only selection, or {@code null} when an explicit status filter excludes
   * FAILED.
   */
  public static JobFilter normalize(JobFilter filter, int limit) {
    Objects.requireNonNull(filter, "filter");
    if (limit < 1 || limit > MAX_LIMIT) {
      throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT);
    }
    if (filter.statuses() != null && !filter.statuses().contains(JobStatus.FAILED)) {
      return null;
    }
    return filter.toBuilder().statuses(JobStatus.FAILED).includeArchived(false).build();
  }
}
