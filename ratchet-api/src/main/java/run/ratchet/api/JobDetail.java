package run.ratchet.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Full read-only view of a job, including bounded execution history and runtime metadata.
 *
 * <p>Returned by {@link JobQueryService#getJobDetail(UUID)}. Execution history and dependant IDs
 * are capped at {@link JobQueryService#DEFAULT_PAGE_LIMIT}; use the paged query methods to walk
 * larger histories or dependency sets. For lightweight list views use {@link JobSummary} instead.
 */
public record JobDetail(
    JobSummary summary,
    Map<String, String> params,
    Map<String, String> traceContext,
    String jobResult,
    String resultType,
    Instant executionStartTime,
    Instant executionEndTime,
    Long executionDurationMs,
    Long queueWaitMs,
    List<ExecutionHistorySummary> executionHistory,
    List<UUID> dependantJobIds) {
  public JobDetail {
    params = params == null ? null : Map.copyOf(params);
    traceContext = traceContext == null ? null : Map.copyOf(traceContext);
    executionHistory = executionHistory == null ? null : List.copyOf(executionHistory);
    dependantJobIds = dependantJobIds == null ? null : List.copyOf(dependantJobIds);
  }
}
