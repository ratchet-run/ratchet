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
