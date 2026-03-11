package run.ratchet.api;

import run.ratchet.spi.JobLogger;
import java.util.Collections;
import java.util.Map;

/**
 * Thread-local context object providing job-specific information during execution.
 *
 * <p>JobContext serves as the primary interface for jobs to access runtime information and services
 * during their execution. It provides access to the job's unique identifier, logging facilities,
 * and configuration parameters. The context is automatically bound to the executing thread and
 * cleared upon completion.
 *
 * <h2>Key Features:</h2>
 *
 * <ul>
 *   <li>Thread-local storage ensuring isolated execution contexts
 *   <li>Access to job-specific logger for structured logging
 *   <li>Parameter access for runtime configuration
 *   <li>Automatic lifecycle management
 * </ul>
 *
 * <h2>Usage in Job Implementation:</h2>
 *
 * <pre>{@code
 * public class DataProcessingJob implements SerializableCheckedRunnable {
 *     public void run() throws Exception {
 *         JobContext ctx = JobContext.current();
 *         JobLogger logger = ctx.logger();
 *
 *         // Access job parameters
 *         String batchSize = ctx.param("batchSize", "100");
 *         String targetTable = ctx.param("targetTable");
 *
 *         logger.info("Processing job {} with batch size {}",
 *                     ctx.jobId(), batchSize);
 *
 *         // Perform job logic...
 *     }
 * }
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 *
 * <p>JobContext uses {@link ThreadLocal} storage to ensure each thread has its own isolated
 * context. This prevents cross-contamination in multi-threaded execution environments where
 * multiple jobs may run concurrently.
 *
 * <h2>Lifecycle:</h2>
 *
 * <ol>
 *   <li>Context is created and bound when job execution begins
 *   <li>Available throughout job execution via {@link #current()}
 *   <li>Automatically cleared when job completes (success or failure)
 * </ol>
 *
 * @see JobLogger
 * @see JobSchedulerService
 */
public final class JobContext {

  /**
   * Thread-local storage for the currently executing job's context.
   *
   * <p>This enables jobs to access their context without explicit parameter passing. The context is
   * bound at the start of job execution via {@link #bind(long, JobLogger)} and must be cleared via
   * {@link #clear()} when execution completes to prevent memory leaks in thread pool environments.
   */
  private static final ThreadLocal<JobContext> TL = new ThreadLocal<>();

  /**
   * The unique identifier of the job associated with this context.
   *
   * <p>This ID is assigned during job creation and remains constant throughout the job's lifecycle.
   * It can be used for correlation in logs, monitoring, and when referencing the job in other
   * system components.
   */
  private final long jobId;

  /**
   * The logger instance configured for this job's execution.
   *
   * <p>Provides structured logging with automatic job context inclusion (job ID, timestamps, etc.)
   * in all log entries for better observability and debugging.
   */
  private final JobLogger logger;

  /**
   * Immutable map of job parameters configured at submission time.
   *
   * <p>Parameters provide lightweight configuration data without complex object serialization.
   * Accessed via {@link #param(String)} and {@link #param(String, String)}. The map is wrapped as
   * unmodifiable to prevent accidental modification during execution.
   */
  private final Map<String, String> params;

  /**
   * Creates a new instance of JobContext with the specified job identifier and logger. This
   * constructor is private and intended for internal use within the JobContext class.
   *
   * @param jobId the unique identifier of the job
   * @param logger the JobLogger instance associated with the job
   */
  private JobContext(long jobId, JobLogger logger) {
    this(jobId, logger, Collections.emptyMap());
  }

  /**
   * Creates a new instance of JobContext with the specified job identifier, logger, and parameters.
   * This constructor is private and intended for internal use within the JobContext class.
   *
   * @param jobId the unique identifier of the job
   * @param logger the JobLogger instance associated with the job
   * @param params the job parameters map
   */
  private JobContext(long jobId, JobLogger logger, Map<String, String> params) {
    this.jobId = jobId;
    this.logger = logger;
    this.params = params != null ? Collections.unmodifiableMap(params) : Collections.emptyMap();
  }

  /**
   * Binds a new {@code JobContext} instance to the current thread for a specific job execution. The
   * method creates a {@code JobContext} using the provided job ID and logger, associates it with
   * the thread using a {@code ThreadLocal}, and returns it.
   *
   * @param jobId the unique identifier of the job
   * @param logger the {@code JobLogger} instance associated with the job
   * @return the newly created and bound {@code JobContext} instance
   */
  public static JobContext bind(long jobId, JobLogger logger) {
    JobContext ctx = new JobContext(jobId, logger);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new {@code JobContext} instance to the current thread for a specific job execution. The
   * method creates a {@code JobContext} using the provided job ID, logger, and parameters,
   * associates it with the thread using a {@code ThreadLocal}, and returns it.
   *
   * @param jobId the unique identifier of the job
   * @param logger the {@code JobLogger} instance associated with the job
   * @param params the job parameters map
   * @return the newly created and bound {@code JobContext} instance
   */
  public static JobContext bind(long jobId, JobLogger logger, Map<String, String> params) {
    JobContext ctx = new JobContext(jobId, logger, params);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Clears the {@link JobContext} bound to the current thread.
   *
   * <p>This method removes the {@link JobContext} instance associated with the current thread from
   * the {@code ThreadLocal}, effectively unbinding the context. Use this method to ensure proper
   * cleanup of thread-specific job contexts, especially when the job execution is complete or when
   * the context is no longer needed. Failure to call this method may result in resource leaks or
   * unintended behavior in subsequent thread reuse.
   */
  public static void clear() {
    TL.remove();
  }

  /**
   * Retrieves the {@code JobContext} instance currently bound to the current thread. If no {@code
   * JobContext} is bound, this method throws an {@code IllegalStateException}.
   *
   * @return the {@code JobContext} instance bound to the current thread
   * @throws IllegalStateException if no {@code JobContext} is bound to the current thread
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
