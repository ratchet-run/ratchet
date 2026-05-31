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

/** Fired when a failed job is about to be retried. */
@Incubating
public class JobRetryingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3500975641125637480L;

  private final String errorMessage;
  private final int retryAttempt;
  private final Instant scheduledTime;

  /**
   * Creates a retry event with an explicit event timestamp.
   *
   * @param errorMessage sanitized failure message that caused the retry, or {@code null} if no
   *     message was recorded
   * @param retryAttempt 1-based execution attempt count being scheduled for retry
   * @param scheduledTime instant when the retry becomes eligible for claiming
   */
  public JobRetryingEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      int retryAttempt,
      Instant scheduledTime) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requirePositive(retryAttempt, "retryAttempt");
    this.scheduledTime = EventContract.requireNonNull(scheduledTime, "scheduledTime");
  }

  /**
   * Creates a retry event using the current system clock instant.
   *
   * <p>Use the constructor that accepts an explicit {@link Instant} for tests, replay, or any retry
   * path that already has a scheduler-provided timestamp.
   *
   * @param errorMessage sanitized failure message that caused the retry, or {@code null} if no
   *     message was recorded
   * @param retryAttempt 1-based execution attempt count being scheduled for retry
   * @param scheduledTime instant when the retry becomes eligible for claiming
   */
  public JobRetryingEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      int retryAttempt,
      Instant scheduledTime) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requirePositive(retryAttempt, "retryAttempt");
    this.scheduledTime = EventContract.requireNonNull(scheduledTime, "scheduledTime");
  }

  /** Returns the sanitized failure message that caused the retry, or {@code null} if absent. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the 1-based execution attempt count being scheduled for retry. */
  public int getRetryAttempt() {
    return retryAttempt;
  }

  /** Returns when the retry is scheduled to become claimable. */
  public Instant getScheduledTime() {
    return scheduledTime;
  }
}
