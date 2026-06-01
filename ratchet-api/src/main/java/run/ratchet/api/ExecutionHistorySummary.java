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
package run.ratchet.api;

import java.time.Instant;
import java.util.UUID;

/**
 * Read-only projection of a single execution attempt for a job.
 *
 * @param id the execution record id
 * @param jobId the parent job id
 * @param attempt one-based attempt number
 * @param nodeId the scheduler node that executed this attempt
 * @param startedAt when execution began
 * @param endedAt when execution finished; null if still running
 * @param durationMs wall-clock duration in milliseconds; null if not yet finished
 * @param succeeded true if this attempt completed without error
 * @param errorMessage the error message if the attempt failed; null otherwise
 * @param errorClass the exception class name if the attempt failed; null otherwise
 * @since 0.1
 */
@Incubating
public record ExecutionHistorySummary(
    UUID id,
    UUID jobId,
    int attempt,
    String nodeId,
    Instant startedAt,
    Instant endedAt,
    Long durationMs,
    boolean succeeded,
    String errorMessage,
    String errorClass) {}
