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
package run.ratchet.store.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

/** Immutable scheduler mutations that must commit with a job's terminal transition. */
public record JobCompletionPlan(
    UUID jobId,
    JobStatus expectedStatus,
    JobStatus terminalStatus,
    String resultJson,
    String resultType,
    String errorMessage,
    int attempts,
    Instant start,
    Instant end,
    Long durationMs,
    Long queueWaitMs,
    UUID batchId,
    BatchCompletion completedBatch,
    List<DependencyTransition> dependencies) {

  public JobCompletionPlan {
    Objects.requireNonNull(jobId);
    Objects.requireNonNull(expectedStatus);
    if (terminalStatus != JobStatus.SUCCEEDED
        && terminalStatus != JobStatus.FAILED
        && terminalStatus != JobStatus.CANCELED) {
      throw new IllegalArgumentException("Completion requires a terminal status");
    }
    dependencies = List.copyOf(dependencies);
    if (batchId != null && completedBatch != null) {
      throw new IllegalArgumentException(
          "A completion cannot be both a batch child and batch parent");
    }
  }

  /**
   * A dependent's snapshot guard and its desired schedule/status; cancellation releases its key.
   */
  public record DependencyTransition(
      UUID jobId,
      JobStatus expectedStatus,
      Integer expectedVersion,
      Instant expectedScheduledTime,
      JobStatus status,
      Instant scheduledTime,
      JobExecutionType jobType) {
    public DependencyTransition {
      Objects.requireNonNull(jobId);
      Objects.requireNonNull(expectedStatus);
      Objects.requireNonNull(status);
      Objects.requireNonNull(jobType);
      if (status != JobStatus.PENDING
          && status != JobStatus.WAITING
          && status != JobStatus.CANCELED) {
        throw new IllegalArgumentException(
            "A dependency must remain pending/waiting or be canceled");
      }
    }
  }

  /** Snapshot guard for completing a synthetic batch parent and its completion flag atomically. */
  public record BatchCompletion(int totalItems, int completedItems, int failedItems) {
    public BatchCompletion {
      if (totalItems < 0
          || completedItems < 0
          || failedItems < 0
          || (long) completedItems + failedItems < totalItems) {
        throw new IllegalArgumentException(
            "Batch completion requires nonnegative, complete counters");
      }
    }
  }
}
