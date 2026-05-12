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
 *
 * @param summary lightweight job summary; never null for a returned detail
 * @param params stored job parameters, or null if none were persisted
 * @param traceContext stored trace context carrier entries, or null if none were persisted
 * @param jobResult serialized job result, or null if the job has not produced one
 * @param resultType fully-qualified result class name, or null when no result is available
 * @param executionStartTime when the current or last execution attempt started, or null if
 *     unstarted
 * @param executionEndTime when the current or last execution attempt ended, or null if still
 *     running or unstarted
 * @param executionDurationMs execution duration in milliseconds, or null if unavailable
 * @param queueWaitMs time from scheduled availability to execution start in milliseconds, or null
 *     if unavailable
 * @param executionHistory bounded execution history for this job; never {@code null}
 * @param dependantJobIds bounded IDs of jobs that directly depend on this job; never {@code null}
 */
@Incubating
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
    executionHistory = executionHistory == null ? List.of() : List.copyOf(executionHistory);
    dependantJobIds = dependantJobIds == null ? List.of() : List.copyOf(dependantJobIds);
  }
}
