package run.ratchet.ri.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import run.ratchet.store.entity.JobLogEntity.LogLevel;

/** Log entry produced during job execution. Carries the MDC snapshot for diagnostic context. */
public record JobLogLine(
    UUID jobId, Instant timestamp, LogLevel level, String message, Map<String, Object> mdc)
    implements Serializable {}
