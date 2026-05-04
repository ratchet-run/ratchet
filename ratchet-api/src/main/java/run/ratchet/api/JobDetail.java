package run.ratchet.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Full read-only view of a job, including execution history and runtime metadata.
 *
 * <p>Returned by {@link JobQueryService#getJobDetail(UUID)}. For lightweight list views use {@link
 * JobSummary} instead.
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
    List<UUID> dependantJobIds) {}
