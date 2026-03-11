package run.ratchet.ri.core;

import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Immutable data carrier for job execution log entries, supporting real-time streaming and
 * persistent storage of job-specific logging information. This record serves as the primary data
 * structure for transmitting log messages between the job execution context, storage layer, and
 * real-time monitoring interfaces.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li><b>Immutability:</b> Thread-safe by design, suitable for concurrent access
 *   <li><b>Serializable:</b> Supports transport and database persistence
 *   <li><b>MDC Support:</b> Carries diagnostic context for troubleshooting
 *   <li><b>Streaming Ready:</b> Designed for Server-Sent Events (SSE) delivery
 * </ul>
 *
 * <p>The MDC (Mapped Diagnostic Context) map can contain:
 *
 * <ul>
 *   <li>Thread information (name, ID)
 *   <li>Node identifier for cluster environments
 *   <li>Job-specific context (business key, retry attempt)
 *   <li>Performance metrics (execution time, memory usage)
 * </ul>
 *
 * @param jobId unique identifier of the job that generated this log entry
 * @param timestamp precise instant when the log event occurred
 * @param level severity level following standard logging hierarchy
 * @param message human-readable log message text
 * @param mdc diagnostic context map for additional troubleshooting data
 */
public record JobLogLine(
    long jobId, Instant timestamp, LogLevel level, String message, Map<String, Object> mdc)
    implements Serializable {}
