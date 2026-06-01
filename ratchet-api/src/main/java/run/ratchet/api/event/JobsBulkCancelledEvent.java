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

/**
 * Fired exactly once per successful bulk cancel-by-tag operation when at least one job was
 * cancelled.
 *
 * <p>Standalone event sibling of the per-job cancellation hierarchy. Bulk cancellation does not
 * carry a single {@code jobId} / {@code businessKey} / {@code priority}, so this event does not
 * extend {@link AbstractJobSchedulerEvent} — observers pattern-matching on per-job fields would
 * fail on bulk events.
 *
 * @see run.ratchet.api.JobSchedulerService#cancelJobsByTag(String)
 * @see run.ratchet.api.JobSchedulerService#cancelRecurringJobsByTag(String)
 */
@Incubating
public class JobsBulkCancelledEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String tag;
  private final int count;
  private final Instant cancelledAt;

  /**
   * Creates a bulk-cancel event.
   *
   * @param tag tag used to select jobs for cancellation
   * @param count number of jobs successfully cancelled
   * @param cancelledAt instant when the bulk operation completed
   */
  public JobsBulkCancelledEvent(String tag, int count, Instant cancelledAt) {
    this.tag = EventContract.requireNonBlank(tag, "tag");
    this.count = EventContract.requirePositive(count, "count");
    this.cancelledAt = EventContract.requireNonNull(cancelledAt, "cancelledAt");
  }

  /** Returns the tag used to select jobs for cancellation. */
  public String getTag() {
    return tag;
  }

  /** Returns the number of jobs successfully cancelled. */
  public int getCount() {
    return count;
  }

  /** Returns the instant when the bulk operation completed. */
  public Instant getCancelledAt() {
    return cancelledAt;
  }
}
