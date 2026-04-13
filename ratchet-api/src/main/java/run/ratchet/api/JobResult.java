package run.ratchet.api;

import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Captures the outcome of a job execution: success/failure status, return value, error details,
 * timing, and custom metadata. Used by workflow conditions for branching decisions.
 *
 * <pre>{@code
 * JobResult<Integer> success = JobResult.success(42);
 * JobResult<Void> failure = JobResult.failure("Connection failed", exception);
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

  @Serial private static final long serialVersionUID = 5109014978656748418L;

  private final boolean success;
  private final T value;
  private final String error;
  private final Throwable exception;
  private final Long executionTimeMs;
  private final Instant startTime;
  private final Instant endTime;
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

  /** Creates a successful result with the given return value. */
  public static <T> JobResult<T> success(T value) {
    return new JobResult<>(true, value, null, null, null, null, null, null);
  }

  /** Creates a failure result with an error message and the causing exception. */
  public static <T> JobResult<T> failure(String error, Throwable exception) {
    return new JobResult<>(false, null, error, exception, null, null, null, null);
  }

  /** Creates a JobResult with all fields specified. */
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

  public long getExecutionTimeMsOrZero() {
    return executionTimeMs != null ? executionTimeMs : 0L;
  }

  /**
   * @param key the metadata key to look up
   * @return the metadata value, or null if not found
   */
  public Object getMetadata(String key) {
    return metadata != null ? metadata.get(key) : null;
  }

  /**
   * Returns a typed metadata value, falling back to the default if absent.
   *
   * @param <V> the expected type
   * @param key the metadata key
   * @param defaultValue fallback if key is not found
   * @return the metadata value or the default
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
   * @return true if an error message or exception is present
   */
  public boolean hasError() {
    return error != null || exception != null;
  }

  /**
   * @return true if the job returned a non-null value
   */
  public boolean hasValue() {
    return value != null;
  }

  public boolean isFailure() {
    return !success;
  }

  public boolean isSuccess() {
    return success;
  }

  /**
   * Ensures serialization safety by converting any non-serializable Throwable in the exception
   * field to a safe RuntimeException that preserves the original class name, message, and stack
   * trace.
   *
   * <p>This is transparent to callers — in-memory access via {@link #getException()} returns the
   * original Throwable. Only the serialized form is sanitized.
   */
  @Serial
  private Object writeReplace() throws ObjectStreamException {
    if (exception == null) {
      return this;
    }
    return new JobResult<>(
        success,
        value,
        error,
        sanitizeThrowable(exception),
        executionTimeMs,
        startTime,
        endTime,
        metadata);
  }

  private static RuntimeException sanitizeThrowable(Throwable t) {
    RuntimeException safe = new RuntimeException(t.getClass().getName() + ": " + t.getMessage());
    safe.setStackTrace(t.getStackTrace());
    if (t.getCause() != null && t.getCause() != t) {
      safe.initCause(sanitizeThrowable(t.getCause()));
    }
    return safe;
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
