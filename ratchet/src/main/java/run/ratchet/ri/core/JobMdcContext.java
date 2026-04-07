package run.ratchet.ri.core;

import run.ratchet.api.JobContext;
import run.ratchet.spi.JobLogger;
import java.util.Map;

/**
 * Binds the per-thread {@link JobContext} for job execution.
 *
 * <p>Previously this class also maintained a private {@code ThreadLocal<Map<String,String>>} named
 * "MDC", but the codebase logs via {@code java.util.logging} which has no MDC concept and nothing
 * in the runtime ever consumed those values. The map was deleted in 0.1.0-alpha along with its
 * {@code setup()}, {@code getMdc()}, and {@code setJobCreator()} helpers. The {@code JobContext}
 * binding (used by user code via {@code JobContext.current()}) remains.
 *
 * <p>When the project migrates to SLF4J (planned for 0.2), this class should be replaced with
 * direct calls to {@link org.slf4j.MDC} alongside the {@code JobContext} binding.
 *
 * @see JobContext
 */
final class JobMdcContext {

  private JobMdcContext() {
    // Utility class
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

  /** Unbinds the thread-local JobContext. Safe to call multiple times. */
  static void clear() {
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
