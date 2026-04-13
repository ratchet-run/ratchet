package run.ratchet.api;

import run.ratchet.spi.JobLogger;
import java.util.Collections;
import java.util.Map;

/** Thread-local context for the executing job. */
public final class JobContext {

  private static final ThreadLocal<JobContext> TL = new ThreadLocal<>();

  private final long jobId;
  private final JobLogger logger;
  private final Map<String, String> params;

  private JobContext(long jobId, JobLogger logger) {
    this(jobId, logger, Collections.emptyMap());
  }

  private JobContext(long jobId, JobLogger logger, Map<String, String> params) {
    this.jobId = jobId;
    this.logger = logger;
    this.params = params != null ? Collections.unmodifiableMap(params) : Collections.emptyMap();
  }

  /**
   * Binds a new context to the current thread. Always pair with {@link #clear()} in a finally
   * block.
   */
  public static JobContext bind(long jobId, JobLogger logger) {
    JobContext ctx = new JobContext(jobId, logger);
    TL.set(ctx);
    return ctx;
  }

  /** Binds a new context with parameters to the current thread. */
  public static JobContext bind(long jobId, JobLogger logger, Map<String, String> params) {
    JobContext ctx = new JobContext(jobId, logger, params);
    TL.set(ctx);
    return ctx;
  }

  /** Removes the context bound to the current thread. */
  public static void clear() {
    TL.remove();
  }

  /**
   * @return the context bound to the current thread
   * @throws IllegalStateException if no context is bound
   */
  public static JobContext current() {
    JobContext ctx = TL.get();
    if (ctx == null) {
      throw new IllegalStateException("No JobContext bound to current thread");
    }
    return ctx;
  }

  public long jobId() {
    return jobId;
  }

  /** Returns the job-scoped logger (automatically includes job ID in all entries). */
  public JobLogger logger() {
    return logger;
  }

  /**
   * @return the parameter value, or null if the key does not exist
   */
  public String param(String key) {
    return params.get(key);
  }

  /**
   * @return the parameter value if present, otherwise {@code defaultValue}
   */
  public String param(String key, String defaultValue) {
    return params.getOrDefault(key, defaultValue);
  }

  public Map<String, String> params() {
    return params;
  }
}
