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
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.Nullable;

/** Immutable terminal event snapshot committed with the primary terminal transition. */
public record OutboxEvent(
    UUID eventId,
    OutboxEventType eventType,
    UUID jobId,
    @Nullable String businessKey,
    @Nullable UUID recurringMasterId,
    @Nullable JobType jobType,
    @Nullable JobPriority priority,
    @Nullable String nodeId,
    Instant eventTimestamp,
    @Nullable Long executionTimeMs,
    @Nullable String errorMessage,
    int retryAttempt,
    Instant createdAt) {
  public OutboxEvent {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(eventType, "eventType");
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    Objects.requireNonNull(createdAt, "createdAt");
    if (retryAttempt < 0) throw new IllegalArgumentException("retryAttempt must be nonnegative");
  }
}
