package run.ratchet.ri.core;

import run.ratchet.spi.JobLogger;
import java.util.logging.Logger;

/**
 * Job-specific logging implementation that uses JUL (java.util.logging) as the backend and
 * publishes log entries through the internal event publisher for persistence and streaming.
 *
 * <p>Each JulJobLogger instance is bound to a specific job ID, ensuring log isolation and
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
 * @see JobLogger
 */
public class JulJobLogger implements JobLogger {

  private static final Logger log = Logger.getLogger(JulJobLogger.class.getName());

  private final long jobId;
  private final InternalEventPublisher eventPublisher;

  /**
   * Creates a new JulJobLogger bound to a specific job ID.
   *
   * @param jobId the unique identifier of the job to bind this logger to
   * @param eventPublisher the event publisher for log line dispatch
   */
  public JulJobLogger(long jobId, InternalEventPublisher eventPublisher) {
    this.jobId = jobId;
    this.eventPublisher = eventPublisher;
  }

  @Override
  public void info(String message) {
    log.info("[Job " + jobId + "] " + message);
    publishLogLine("INFO", message);
  }

  @Override
  public void debug(String message) {
    log.fine("[Job " + jobId + "] " + message);
    publishLogLine("DEBUG", message);
  }

  @Override
  public void warn(String message) {
    log.warning("[Job " + jobId + "] " + message);
    publishLogLine("WARN", message);
  }

  @Override
  public void error(String message) {
    log.severe("[Job " + jobId + "] " + message);
    publishLogLine("ERROR", message);
  }

  @Override
  public void trace(String message) {
    log.finest("[Job " + jobId + "] " + message);
    publishLogLine("TRACE", message);
  }

  private void publishLogLine(String level, String message) {
    if (eventPublisher != null) {
      eventPublisher.publish(new JobLogLine(jobId, level, message));
    }
  }

  /**
   * Simple log line record for publishing through the event system.
   *
   * @param jobId the job that produced this log entry
   * @param level the log level
   * @param message the log message
   */
  public record JobLogLine(long jobId, String level, String message) {}
}
