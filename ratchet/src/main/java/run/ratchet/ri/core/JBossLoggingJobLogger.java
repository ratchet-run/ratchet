package run.ratchet.ri.core;

import run.ratchet.spi.JobLogger;
import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Per-job {@link JobLogger} backed by JBoss Logging, with log lines published through {@link
 * InternalEventPublisher} for persistence. Each instance is bound to a single job ID.
 *
 * <p>Reserved for future per-job logger wiring; not currently instantiated by {@code JobTask}.
 */
public class JBossLoggingJobLogger implements JobLogger {

  private static final Logger log = Logger.getLogger(JBossLoggingJobLogger.class);

  private final long jobId;
  private final InternalEventPublisher eventPublisher;

  /**
   * Creates a new JBossLoggingJobLogger bound to a specific job ID.
   *
   * @param jobId the unique identifier of the job to bind this logger to
   * @param eventPublisher the event publisher for log line dispatch
   */
  public JBossLoggingJobLogger(long jobId, InternalEventPublisher eventPublisher) {
    this.jobId = jobId;
    this.eventPublisher = eventPublisher;
  }

  @Override
  public void info(String message) {
    log.infof("[Job %d] %s", jobId, message);
    publishLogLine(LogLevel.INFO, message);
  }

  @Override
  public void debug(String message) {
    log.debugf("[Job %d] %s", jobId, message);
    publishLogLine(LogLevel.DEBUG, message);
  }

  @Override
  public void warn(String message) {
    log.warnf("[Job %d] %s", jobId, message);
    publishLogLine(LogLevel.WARN, message);
  }

  @Override
  public void error(String message) {
    log.errorf("[Job %d] %s", jobId, message);
    publishLogLine(LogLevel.ERROR, message);
  }

  @Override
  public void trace(String message) {
    log.tracef("[Job %d] %s", jobId, message);
    publishLogLine(LogLevel.TRACE, message);
  }

  private void publishLogLine(LogLevel level, String message) {
    if (eventPublisher != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> mdcSnapshot =
          MDC.getMap() == null ? new HashMap<>() : new HashMap<>(MDC.getMap());
      eventPublisher.publish(new JobLogLine(jobId, Instant.now(), level, message, mdcSnapshot));
    }
  }
}
