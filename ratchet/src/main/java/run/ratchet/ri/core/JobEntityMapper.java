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
package run.ratchet.ri.core;

import java.util.Collections;
import java.util.List;
import run.ratchet.api.ExecutionHistorySummary;
import run.ratchet.api.JobSummary;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;

/** Static mapping utilities from store entities to public read-model types. */
final class JobEntityMapper {

  private JobEntityMapper() {}

  static JobSummary toSummary(JobEntity e) {
    return new JobSummary(
        e.getId(),
        e.getStatus(),
        e.getJobType() != null ? e.getJobType().toPublicType() : null,
        e.getPriority(),
        e.getBusinessKey(),
        e.getIdempotencyKey(),
        e.getTargetClass(),
        e.getMethodName(),
        e.getTags() != null ? List.copyOf(e.getTags()) : Collections.emptyList(),
        e.getResourceName(),
        e.getPickedBy(),
        e.getCreatedAt(),
        e.getScheduledTime(),
        e.getUpdatedAt(),
        e.getCallerPrincipal(),
        e.getLastError(),
        e.getAttempts(),
        e.getMaxRetries(),
        e.getDependsOn());
  }

  static ExecutionHistorySummary toExecutionSummary(JobExecutionEntity e) {
    boolean succeeded = e.getStatus() == JobExecutionEntity.ExecutionStatus.SUCCEEDED;
    return new ExecutionHistorySummary(
        e.getId(),
        e.getJobId(),
        e.getAttempt(),
        e.getNodeId(),
        e.getStartedAt(),
        e.getEndedAt(),
        e.getDurationMs(),
        succeeded,
        e.getErrorMessage(),
        e.getErrorClass());
  }
}
