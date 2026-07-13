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
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when all children of a batch reach a terminal state.
 *
 * <p>{@code completedItems} counts successful children and {@code failedItems} counts children that
 * ended in a failed state. The event is still emitted when one or more children failed, after the
 * batch has no remaining pending/running children.
 */
@Incubating
public class BatchCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 843735174177646423L;

  private final int totalItems;
  private final int completedItems;
  private final int failedItems;

  public BatchCompletedEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      int totalItems,
      int completedItems,
      int failedItems) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId, timestamp);
    EventContract.requireBatchCounts(totalItems, completedItems, failedItems);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  public BatchCompletedEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      int totalItems,
      int completedItems,
      int failedItems) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId);
    EventContract.requireBatchCounts(totalItems, completedItems, failedItems);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  /** Returns the total number of child jobs in the batch. */
  public int getTotalItems() {
    return totalItems;
  }

  /** Returns the number of child jobs that completed successfully. */
  public int getCompletedItems() {
    return completedItems;
  }

  /** Returns the number of child jobs that failed. */
  public int getFailedItems() {
    return failedItems;
  }
}
