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
package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobFilter;

/**
 * Fired once after a successful bounded bulk retry resets at least one failed job.
 *
 * <p>This aggregate event carries the caller's selection and the number of jobs reset. Bulk retry
 * does not publish a {@link JobRetryingEvent} for every selected job.
 *
 * @see run.ratchet.api.JobSchedulerService#retryJobs(JobFilter, int)
 */
@Incubating
public class JobsBulkRetriedEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final JobFilter filter;
  private final int limit;
  private final int count;
  private final Instant retriedAt;

  /**
   * Creates a bulk-retry event.
   *
   * @param filter selection requested by the caller
   * @param limit maximum jobs allowed in the recovery batch, from 1 through 1000
   * @param count number of failed jobs reset to PENDING
   * @param retriedAt instant when the bulk operation completed
   */
  public JobsBulkRetriedEvent(JobFilter filter, int limit, int count, Instant retriedAt) {
    this.filter = EventContract.requireNonNull(filter, "filter");
    if (limit < 1 || limit > 1000) {
      throw new IllegalArgumentException("limit must be between 1 and 1000");
    }
    this.limit = limit;
    this.count = EventContract.requirePositive(count, "count");
    if (count > limit) {
      throw new IllegalArgumentException("count must not exceed limit");
    }
    this.retriedAt = EventContract.requireNonNull(retriedAt, "retriedAt");
  }

  /** Returns the selection requested by the caller. */
  public JobFilter getFilter() {
    return filter;
  }

  /** Returns the maximum jobs allowed in the recovery batch. */
  public int getLimit() {
    return limit;
  }

  /** Returns the number of failed jobs reset to PENDING. */
  public int getCount() {
    return count;
  }

  /** Returns the instant when the bulk operation completed. */
  public Instant getRetriedAt() {
    return retriedAt;
  }
}
