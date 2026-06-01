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
 * Fired when a job reaches terminal FAILED state.
 *
 * <p>Retryable per-attempt failures are reported through the metrics SPI and {@link
 * JobRetryingEvent}; they do not publish this event unless the failed attempt exhausts retry
 * handling and terminalizes the job.
 */
@Incubating
public class JobFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8745178784765705117L;

  private final String errorMessage;
  private final int retryAttempt;

  /**
   * Creates a failure event with an explicit timestamp.
   *
   * @param errorMessage sanitized failure message, or {@code null} when no message was recorded
   * @param retryAttempt recorded retry count when the job failed; zero means no retry was consumed
   */
  public JobFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /**
   * Creates a failure event using the current system clock instant.
   *
   * @param errorMessage sanitized failure message, or {@code null} when no message was recorded
   * @param retryAttempt recorded retry count when the job failed; zero means no retry was consumed
   */
  public JobFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /** Returns the sanitized failure message, or {@code null} when no message was recorded. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the recorded retry count when the job failed. */
  public int getRetryAttempt() {
    return retryAttempt;
  }
}
