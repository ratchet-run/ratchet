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

/** Fired after Ratchet moves a permanently failed job to the dead letter queue. */
@Incubating
public class JobDlqEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -2578972098474327757L;

  private final String errorMessage;
  private final int retryAttempt;

  /**
   * Creates an event with an explicit timestamp.
   *
   * @param errorMessage sanitized final failure message
   * @param retryAttempt final recorded retry count before DLQ; zero means no retry was consumed
   */
  public JobDlqEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /**
   * Creates an event using the current system clock instant.
   *
   * <p>Tests that assert event timestamps should use the constructor that accepts an explicit
   * {@link Instant}.
   *
   * @param errorMessage sanitized final failure message
   * @param retryAttempt final recorded retry count before DLQ; zero means no retry was consumed
   */
  public JobDlqEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /** Returns the sanitized final failure message. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the final recorded retry count before DLQ; zero means no retry was consumed. */
  public int getRetryAttempt() {
    return retryAttempt;
  }
}
