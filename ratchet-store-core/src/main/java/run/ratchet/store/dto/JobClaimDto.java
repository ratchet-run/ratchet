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

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Lightweight DTO for job claiming operations.
 *
 * <p>Contains only the metadata fields needed during the claim phase. Large fields (payload,
 * params, jobResult, lastError) are NOT included to reduce data transfer during polling.
 *
 * @param id job primary key; never {@code null}
 * @param status current job status; never {@code null}
 * @param jobType internal execution type; never {@code null}
 * @param priority job priority; never {@code null}
 * @param scheduledTime time at which the job becomes eligible for pickup
 * @param version optimistic lock version
 * @param timeoutSec timeout for execution, in seconds
 * @param pickedBy node id that currently owns the claim, or {@code null} when the job has not been
 *     picked up
 * @param pickedAt timestamp at which the claim was acquired, or {@code null} when the job has not
 *     been picked up
 * @param businessKey caller-supplied business key, or {@code null} when none was provided
 * @param attempts retry attempts consumed so far
 * @param maxRetries maximum retry attempts permitted
 */
public record JobClaimDto(
    UUID id,
    JobStatus status,
    JobExecutionType jobType,
    JobPriority priority,
    Instant scheduledTime,
    Integer version,
    int timeoutSec,
    @Nullable String pickedBy,
    @Nullable Instant pickedAt,
    @Nullable String businessKey,
    int attempts,
    int maxRetries,
    String executionTarget)
    implements Serializable {

  /** Returns true if id, status, and jobType are all non-null. */
  public boolean isValid() {
    return id != null && status != null && jobType != null;
  }

  public JobType publicJobType() {
    return jobType.toPublicType();
  }

  /** Returns true if attempts &lt; maxRetries. */
  public boolean hasRetriesRemaining() {
    return attempts < maxRetries;
  }
}
