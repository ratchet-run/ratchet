package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Comprehensive result object capturing all aspects of a job execution.
 *
 * <p>JobResult provides a complete picture of job execution outcomes, including success/failure
 * status, return values, error details, timing information, and custom metadata. This rich
 * information enables sophisticated workflow decisions and detailed monitoring/debugging
 * capabilities.
 *
 * <h2>Key Components:</h2>
 *
 * <ul>
 *   <li><b>Status</b> - Success/failure boolean indicator
 *   <li><b>Return Value</b> - Generic typed result from successful execution
 *   <li><b>Error Information</b> - Message and exception details for failures
 *   <li><b>Timing Data</b> - Start time, end time, and duration
 *   <li><b>Metadata</b> - Extensible key-value pairs for custom data
 * </ul>
 *
 * <h2>Usage in Workflow Conditions:</h2>
 *
 * <pre>{@code
 * // Success-based branching
 * .when(result -> result.isSuccess(), () -> processSuccessPath())
 *
 * // Value-based branching
 * .whenResult(result -> result.getValue() > threshold,
 *            () -> handleHighValue())
 *
 * // Performance-based branching
 * .when(result -> result.getExecutionTimeMs() > 60000,
 *       () -> alertSlowExecution())
 *
 * // Metadata-based branching
 * .when(result -> "critical".equals(result.getMetadata("severity")),
 *       () -> escalateToOpsTeam())
 * }</pre>
 *
 * <h2>Building Results:</h2>
 *
 * <pre>{@code
 * // Success result with value
 * JobResult<Integer> success = JobResult.success(42);
 *
 * // Failure result with error
 * JobResult<Void> failure = JobResult.failure("Database connection failed", dbException);
 * }</pre>
 *
 * @param <T> the type of the job's return value
 * @see WorkflowCondition
 * @see JobBuilder#whenResult(SerializableFunction, SerializableCheckedRunnable)
 */
@SuppressWarnings({"java:S1948"
  // Non-serializable fields are intentional - callers must ensure contents are serializable (see
  // Javadoc)
})
public class JobResult<T> implements Serializable {

  /**
   * Serial version UID for ensuring serialization compatibility across versions.
   *
   * <p>This fixed value ensures that JobResult objects serialized with one version of the class can
   * be deserialized with another version, which is critical for job results that may be persisted
   * or transmitted across cluster nodes.
   */
  @Serial private static final long serialVersionUID = 5109014978656748418L;

  /**
   * Indicates whether the job completed successfully without throwing an exception.
   *
   * <p>A value of {@code true} means the job executed to completion without errors. A value of
   * {@code false} indicates the job threw an exception or was otherwise terminated abnormally. This
   * field is the primary indicator for workflow branching decisions.
   */
  private final boolean success;

  /**
   * The return value from the job method execution.
   *
   * <p>This field holds the result returned by the job's task method. For void methods or failed
   * executions, this will be {@code null}. The generic type {@code T} allows type-safe access to
   * the return value in workflow conditions and downstream processing.
   *
   * <p>Note: The value must be serializable if the result is persisted or transmitted.
   */
  private final T value;

  /**
   * Human-readable error message describing the failure cause.
   *
   * <p>When a job fails, this field contains a summary of what went wrong. It is typically derived
   * from the exception message but may be augmented with additional context. This field is intended
   * for logging, debugging, and user-facing error displays.
   */
  private final String error;

  /**
   * The full exception object thrown by the job, if any.
   *
   * <p>Unlike {@link #error} which contains just the message, this field preserves the complete
   * exception including stack trace, cause chain, and any custom exception data. This is invaluable
   * for debugging but may not serialize well across all contexts.
   *
   * <p><b>Warning:</b> Some exception types may not be serializable. When persisting job results,
   * consider extracting relevant information before serialization.
   */
  private final Throwable exception;

  /**
   * Total execution time of the job in milliseconds.
   *
   * <p>Measures the wall-clock time from when job execution began to when it completed
   * (successfully or not). This metric is useful for performance monitoring, SLA tracking, and
   * identifying slow jobs. May be {@code null} if timing information was not recorded.
   */
  private final Long executionTimeMs;

  /**
   * Timestamp when job execution began.
   *
   * <p>Records the instant when the job worker started executing the task. Combined with {@link
   * #endTime}, this allows calculation of duration and provides temporal context for correlation
   * with other system events and logs.
   */
  private final Instant startTime;

  /**
   * Timestamp when job execution completed.
   *
   * <p>Records the instant when the job finished, whether successfully or due to failure. This
   * timestamp is captured after the task method returns or throws, providing an accurate end point
   * for duration calculations.
   */
  private final Instant endTime;

  /**
   * Extensible key-value map for custom execution metadata.
   *
   * <p>Allows jobs to attach arbitrary data to their results without modifying the JobResult
   * structure. Common uses include:
   *
   * <ul>
   *   <li>Record counts: {@code metadata.put("recordsProcessed", 1000)}
   *   <li>Resource usage: {@code metadata.put("memoryUsedMB", 256)}
   *   <li>Custom status: {@code metadata.put("severity", "critical")}
   *   <li>Diagnostic data: {@code metadata.put("affectedEntities", list)}
   * </ul>
   *
   * <p>Metadata values should be serializable if the result will be persisted.
   */
  private final Map<String, Object> metadata;

  private JobResult(
      boolean success,
      T value,
      String error,
      Throwable exception,
      Long executionTimeMs,
      Instant startTime,
      Instant endTime,
      Map<String, Object> metadata) {
    this.success = success;
    this.value = value;
    this.error = error;
    this.exception = exception;
    this.executionTimeMs = executionTimeMs;
    this.startTime = startTime;
    this.endTime = endTime;
    this.metadata = metadata;
  }

  /**
   * Creates a successful result with the given value.
   *
   * @param value the return value from the job
   * @param <T> the type of the return value
   * @return a successful JobResult containing the value
   */
  public static <T> JobResult<T> success(T value) {
    return new JobResult<>(true, value, null, null, null, null, null, null);
  }

  /**
   * Creates a failure result with the given error message and exception.
   *
   * @param error human-readable error message
   * @param exception the exception that caused the failure
   * @param <T> the type of the return value
   * @return a failed JobResult containing the error details
   */
  public static <T> JobResult<T> failure(String error, Throwable exception) {
    return new JobResult<>(false, null, error, exception, null, null, null, null);
  }

  /**
   * Creates a JobResult with all fields specified.
   *
   * @param success whether the job completed successfully
   * @param value the return value from the job
   * @param error human-readable error message
   * @param exception the exception that caused the failure, if any
   * @param executionTimeMs total execution time in milliseconds
   * @param startTime timestamp when execution began
   * @param endTime timestamp when execution completed
   * @param metadata extensible key-value pairs for custom data
   * @param <T> the type of the return value
   * @return a JobResult with all fields populated
   */
  public static <T> JobResult<T> of(
      boolean success,
      T value,
      String error,
      Throwable exception,
      Long executionTimeMs,
      Instant startTime,
      Instant endTime,
      Map<String, Object> metadata) {
    return new JobResult<>(
        success, value, error, exception, executionTimeMs, startTime, endTime, metadata);
  }

  public T getValue() {
    return value;
  }

  public String getError() {
    return error;
  }

  public Throwable getException() {
    return exception;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  /**
   * Returns the execution time in milliseconds, or 0 if not recorded.
   *
   * <p>This convenience method ensures a non-null return value, making it safe to use in
   * calculations without null checking.
   *
   * @return execution time in milliseconds, or 0 if not available
   */
  public long getExecutionTimeMsOrZero() {
    return executionTimeMs != null ? executionTimeMs : 0L;
  }

  /**
   * Retrieves a metadata value by its key.
   *
   * <p>Metadata provides a flexible way to attach custom information to job results without
   * modifying the core result structure.
   *
   * @param key the metadata key to look up
   * @return the metadata value, or null if not found
   */
  public Object getMetadata(String key) {
    return metadata != null ? metadata.get(key) : null;
  }

  /**
   * Retrieves a typed metadata value with a fallback default.
   *
   * <p>This method provides type-safe access to metadata with automatic casting and a default value
   * if the key is not found or the metadata map is null.
   *
   * <h3>Example:</h3>
   *
   * <pre>{@code
   * Integer count = result.getMetadata("processedCount", 0);
   * String status = result.getMetadata("status", "unknown");
   * }</pre>
   *
   * @param <V> the expected type of the metadata value
   * @param key the metadata key to look up
   * @param defaultValue the value to return if key is not found
   * @return the metadata value if present, otherwise the default value
   */
  @SuppressWarnings("unchecked")
  public <V> V getMetadata(String key, V defaultValue) {
    if (metadata == null) {
      return defaultValue;
    }
    V metadataValue = (V) metadata.get(key);
    return metadataValue != null ? metadataValue : defaultValue;
  }

  /**
   * Checks if the job failed with error information.
   *
   * <p>Returns true if either an error message or exception is present. This can be used to
   * determine if error details are available for logging or debugging purposes.
   *
   * @return true if error information is available
   */
  public boolean hasError() {
    return error != null || exception != null;
  }

  /**
   * Checks if the job produced a return value.
   *
   * <p>Note that a job can be successful without returning a value (e.g., void methods). This
   * method only checks for non-null values.
   *
   * @return true if the job returned a non-null value
   */
  public boolean hasValue() {
    return value != null;
  }

  /**
   * Checks if the job execution failed.
   *
   * <p>This is a convenience method equivalent to {@code !isSuccess()}. Useful in workflow
   * conditions and error handling logic.
   *
   * @return true if the job failed, false if successful
   */
  public boolean isFailure() {
    return !success;
  }

  /**
   * Checks if the job execution completed successfully.
   *
   * <p>A job is considered successful when it completes without throwing an exception, regardless
   * of whether it returns a value.
   *
   * @return true if the job succeeded, false if it failed
   */
  public boolean isSuccess() {
    return success;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobResult<?> that = (JobResult<?>) o;
    return success == that.success
        && Objects.equals(value, that.value)
        && Objects.equals(error, that.error)
        && Objects.equals(exception, that.exception)
        && Objects.equals(executionTimeMs, that.executionTimeMs)
        && Objects.equals(startTime, that.startTime)
        && Objects.equals(endTime, that.endTime)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        success, value, error, exception, executionTimeMs, startTime, endTime, metadata);
  }

  @Override
  public String toString() {
    return "JobResult("
        + "success="
        + success
        + ", value="
        + value
        + ", error="
        + error
        + ", exception="
        + exception
        + ", executionTimeMs="
        + executionTimeMs
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", metadata="
        + metadata
        + ')';
  }
}
