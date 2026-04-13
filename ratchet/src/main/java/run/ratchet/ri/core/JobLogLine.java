package run.ratchet.ri.core;

import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/** Log entry produced during job execution. Carries the MDC snapshot for diagnostic context. */
public record JobLogLine(
    long jobId, Instant timestamp, LogLevel level, String message, Map<String, Object> mdc)
    implements Serializable {}
