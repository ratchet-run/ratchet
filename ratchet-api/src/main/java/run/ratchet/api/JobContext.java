package run.ratchet.api;

import run.ratchet.spi.JobLogger;
import java.util.Collections;
import java.util.Map;

/**
 * Thread-local context providing the current job's ID, logger, and parameters during execution.
 * Bound automatically at job start via {@link #bind} and cleared on completion via {@link #clear}.
 *
 * @see JobLogger
 * @see JobSchedulerService
 */
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

  /**
   * Returns the unique identifier of the currently executing job.
   *
   * <p>This ID can be used for correlation in logs, monitoring systems, or when referencing the job
   * in other parts of the system.
   *
   * @return the job's unique identifier
   */
  public long jobId() {
    return jobId;
  }

  /**
   * Returns the logger instance associated with this job execution.
   *
   * <p>The logger automatically includes job context information (job ID, timestamps) in all log
   * entries, providing structured logging for better observability.
   *
   * @return the job-specific logger instance
   * @see JobLogger
   */
  public JobLogger logger() {
    return logger;
  }

  /**
   * Retrieves a specific parameter value by its key.
   *
   * <p>This method provides direct access to job parameters configured via {@link
   * JobBuilder#withParam(String, String)}.
   *
   * @param key the parameter key to look up
   * @return the parameter value, or null if the key does not exist
   */
  public String param(String key) {
    return params.get(key);
  }

  /**
   * Retrieves a parameter value with a fallback default.
   *
   * <p>This method is useful for optional parameters where a sensible default can be provided if
   * the parameter was not explicitly set.
   *
   * <h3>Example:</h3>
   *
   * <pre>{@code
   * String batchSize = ctx.param("batchSize", "100");
   * String timeout = ctx.param("timeout", "30000");
   * }</pre>
   *
   * @param key the parameter key to look up
   * @param defaultValue the value to return if the key is not found
   * @return the parameter value if present, otherwise the default value
   */
  public String param(String key, String defaultValue) {
    return params.getOrDefault(key, defaultValue);
  }

  /**
   * Returns all parameters configured for this job.
   *
   * <p>Parameters provide a lightweight way to pass configuration data to jobs without the overhead
   * of serializing complex objects. The returned map is unmodifiable to prevent accidental
   * modifications during execution.
   *
   * @return an unmodifiable map of job parameters, never null
   */
  public Map<String, String> params() {
    return params;
  }
}
