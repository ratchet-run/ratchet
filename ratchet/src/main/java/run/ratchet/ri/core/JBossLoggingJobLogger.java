package run.ratchet.ri.core;

import run.ratchet.spi.JobLogger;
import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Job-specific logging implementation that uses JBoss Logging as the backend and publishes log
 * entries through the internal event publisher for persistence and streaming.
 *
 * <p>Each JBossLoggingJobLogger instance is bound to a specific job ID, ensuring log isolation and
 * traceability in concurrent execution environments.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li><b>Job Isolation:</b> Each logger instance is tied to a single job ID
 *   <li><b>Event Publishing:</b> Logs flow through the InternalEventPublisher for persistence
 *   <li><b>SPI Implementation:</b> Implements the {@link JobLogger} SPI interface
 * </ul>
 *
 * <p>Reserved for future per-job logger wiring; not currently instantiated by {@code JobTask}. The
 * {@link JobLogger} SPI is wired via a no-op implementation by default. See plan
 * ratchet-0.2.0-deferred.md for the broader logger-pipeline work.
 *
 * @see JobLogger
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
