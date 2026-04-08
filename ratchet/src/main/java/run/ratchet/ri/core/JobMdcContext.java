package run.ratchet.ri.core;

import run.ratchet.api.JobContext;
import run.ratchet.spi.JobLogger;
import java.util.Map;
import org.jboss.logging.MDC;

/**
 * Binds the per-thread {@link JobContext} for job execution and populates JBoss Logging {@link MDC}
 * keys for log correlation.
 *
 * <p>The MDC keys this class manages — {@link #MDC_JOB_ID}, {@link #MDC_NODE}, and {@link
 * #MDC_JOB_CREATOR} — are written on {@link #bindJobContext(Long, JobLogger, Map, String, String)}
 * and removed (per-key, not via {@code MDC.clear()}) on {@link #clear()}. Per-key removal is
 * deliberate: the enclosing application may have set its own MDC keys (e.g. a request-correlation
 * ID set by a Servlet filter or JAX-RS interceptor) before the job was submitted, and {@code
 * MDC.clear()} would wipe them.
 *
 * <p>JBoss Logging propagates these keys into the active backend's MDC at log emission time. Under
 * JBoss LogManager (WildFly default) and SLF4J/Logback they are rendered via {@code %X{jobId}} etc.
 * Under bare JDK {@code java.util.logging}, MDC values are stored but not rendered by stock JUL
 * formatters.
 *
 * @see JobContext
 */
final class JobMdcContext {

  static final String MDC_JOB_ID = "jobId";
  static final String MDC_NODE = "node";
  static final String MDC_JOB_CREATOR = "jobCreator";

  private JobMdcContext() {
    // Utility class
  }

  /**
   * Binds the thread-local JobContext for the current execution using a no-op logger and no MDC
   * extras. Useful for early-load failure paths where node/creator metadata is not yet available.
   *
   * @param jobId the job identifier
   * @param params the job parameters map
   */
  static void bindJobContext(Long jobId, Map<String, String> params) {
    bindJobContext(jobId, NoOpJobLogger.INSTANCE, params, null, null);
  }

  /**
   * Binds the thread-local JobContext using the no-op logger and populates the MDC with the given
   * node and creator identifiers. The most common entry point from {@code JobTask}.
   *
   * @param jobId the job identifier
   * @param params the job parameters map
   * @param nodeId optional node identifier
   * @param jobCreator optional creator identifier
   */
  static void bindJobContext(
      Long jobId, Map<String, String> params, String nodeId, String jobCreator) {
    bindJobContext(jobId, NoOpJobLogger.INSTANCE, params, nodeId, jobCreator);
  }

  /**
   * Binds the thread-local JobContext and populates the JBoss Logging MDC for log correlation.
   *
   * @param jobId the job identifier (also written to MDC under {@link #MDC_JOB_ID})
   * @param logger the job logger instance
   * @param params the job parameters map
   * @param nodeId optional node identifier (written to MDC under {@link #MDC_NODE} when non-null)
   * @param jobCreator optional creator identifier (written to MDC under {@link #MDC_JOB_CREATOR}
   *     when non-null)
   */
  static void bindJobContext(
      Long jobId, JobLogger logger, Map<String, String> params, String nodeId, String jobCreator) {
    JobContext.bind(jobId, logger, params);
    if (jobId != null) {
      MDC.put(MDC_JOB_ID, String.valueOf(jobId));
    }
    if (nodeId != null) {
      MDC.put(MDC_NODE, nodeId);
    }
    if (jobCreator != null) {
      MDC.put(MDC_JOB_CREATOR, jobCreator);
    }
  }

  /**
   * Unbinds the thread-local JobContext and removes the Ratchet-owned MDC keys. Safe to call
   * multiple times. Removes only Ratchet's own keys ({@link #MDC_JOB_ID}, {@link #MDC_NODE}, {@link
   * #MDC_JOB_CREATOR}) so enclosing-application MDC keys (request IDs, tenant IDs) are preserved.
   */
  static void clear() {
    JobContext.clear();
    MDC.remove(MDC_JOB_ID);
    MDC.remove(MDC_NODE);
    MDC.remove(MDC_JOB_CREATOR);
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
