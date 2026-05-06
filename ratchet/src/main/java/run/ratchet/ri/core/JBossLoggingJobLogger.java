package run.ratchet.ri.core;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import run.ratchet.spi.JobLogger;
import run.ratchet.store.entity.JobLogEntity.LogLevel;

/**
 * Per-job {@link JobLogger} backed by JBoss Logging, with log lines published through {@link
 * InternalEventPublisher} for persistence. Each instance is bound to a single job ID.
 *
 * <p>Created by {@link DefaultJobLoggerFactory} and bound into {@code JobContext} by {@link
 * JobTask}.
 */
public class JBossLoggingJobLogger implements JobLogger {

  private static final Logger log = Logger.getLogger(JBossLoggingJobLogger.class);

  private final UUID jobId;
  private final InternalEventPublisher eventPublisher;

  public JBossLoggingJobLogger(UUID jobId, InternalEventPublisher eventPublisher) {
    this.jobId = jobId;
    this.eventPublisher = eventPublisher;
  }

  @Override
  public void info(String message) {
    log.infof("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.INFO, message);
  }

  @Override
  public void debug(String message) {
    log.debugf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.DEBUG, message);
  }

  @Override
  public void warn(String message) {
    log.warnf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.WARN, message);
  }

  @Override
  public void error(String message) {
    log.errorf("[Job %s] %s", jobId, message);
    publishLogLine(LogLevel.ERROR, message);
  }

  @Override
  public void trace(String message) {
    log.tracef("[Job %s] %s", jobId, message);
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
