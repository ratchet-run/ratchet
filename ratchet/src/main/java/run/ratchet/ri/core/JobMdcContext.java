package run.ratchet.ri.core;

import run.ratchet.api.JobContext;
import run.ratchet.spi.JobLogger;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages MDC (Mapped Diagnostic Context) and JobContext for job execution threads.
 *
 * <p>This utility centralizes all logging context operations. It provides setup and teardown of MDC
 * keys used for distributed tracing and log correlation, as well as the thread-local {@link
 * JobContext} binding.
 *
 * <p>MDC keys managed:
 *
 * <ul>
 *   <li>{@code jobId} -- the job being executed
 *   <li>{@code node} -- the cluster node running the job
 *   <li>{@code jobCreator} -- the user who created the job (if available)
 * </ul>
 *
 * @see JobContext
 */
final class JobMdcContext {

  private static final ThreadLocal<Map<String, String>> MDC = ThreadLocal.withInitial(HashMap::new);

  private JobMdcContext() {
    // Utility class
  }

  /**
   * Returns the current MDC map for the calling thread.
   *
   * @return the current MDC map (never null)
   */
  static Map<String, String> getMdc() {
    return MDC.get();
  }

  /**
   * Sets up MDC context for a job execution thread.
   *
   * @param jobId the job identifier
   * @param nodeId the cluster node identifier
   */
  static void setup(Long jobId, String nodeId) {
    MDC.get().put("jobId", String.valueOf(jobId));
    MDC.get().put("node", nodeId);
  }

  /**
   * Binds the thread-local JobContext for the current execution using a no-op logger.
   *
   * @param jobId the job identifier
   * @param params the job parameters map
   */
  static void bindJobContext(Long jobId, Map<String, String> params) {
    bindJobContext(jobId, NoOpJobLogger.INSTANCE, params);
  }

  /**
   * Binds the thread-local JobContext for the current execution.
   *
   * @param jobId the job identifier
   * @param logger the job logger instance
   * @param params the job parameters map
   */
  static void bindJobContext(Long jobId, JobLogger logger, Map<String, String> params) {
    JobContext.bind(jobId, logger, params);
  }

  /**
   * Adds the job creator to the MDC context for audit trail logging.
   *
   * @param createdBy the username of the job creator, may be null
   */
  static void setJobCreator(String createdBy) {
    if (createdBy != null) {
      MDC.get().put("jobCreator", createdBy);
    }
  }

  /**
   * Clears all MDC keys and unbinds the thread-local JobContext. Safe to call even if setup was not
   * called or was only partially completed.
   */
  static void clear() {
    MDC.remove();
    JobContext.clear();
  }

  /** Minimal no-op logger used when no explicit logger is provided. */
  private enum NoOpJobLogger implements JobLogger {
    INSTANCE;

    @Override
    public void info(String message) {}

    @Override
    public void debug(String message) {}

    @Override
    public void warn(String message) {}

    @Override
    public void error(String message) {}

    @Override
    public void trace(String message) {}
  }
}
