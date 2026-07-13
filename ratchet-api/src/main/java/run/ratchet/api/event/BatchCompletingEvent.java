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
 * Fired after all batch children are terminal and before the batch completion event is published.
 */
@Incubating
public class BatchCompletingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 2629383623872540166L;

  private final int totalItems;
  private final int completedItems;
  private final int failedItems;

  /**
   * Creates a batch-completing event with an explicit timestamp.
   *
   * @param totalItems total number of child jobs in the batch
   * @param completedItems number of successful child jobs in the completed batch
   * @param failedItems number of failed child jobs in the completed batch
   */
  public BatchCompletingEvent(
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

  /**
   * Creates a batch-completing event using the current system clock instant.
   *
   * @param totalItems total number of child jobs in the batch
   * @param completedItems number of successful child jobs in the completed batch
   * @param failedItems number of failed child jobs in the completed batch
   */
  public BatchCompletingEvent(
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

  /** Returns the number of successful child jobs in the completed batch. */
  public int getCompletedItems() {
    return completedItems;
  }

  /** Returns the number of failed child jobs in the completed batch. */
  public int getFailedItems() {
    return failedItems;
  }
}
