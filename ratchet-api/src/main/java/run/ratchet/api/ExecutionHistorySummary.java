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
 */
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
