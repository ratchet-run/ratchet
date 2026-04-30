package run.ratchet.ri.core;

import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Log entry produced during job execution. Carries the MDC snapshot for diagnostic context. */
public record JobLogLine(
    UUID jobId, Instant timestamp, LogLevel level, String message, Map<String, Object> mdc)
    implements Serializable {}
