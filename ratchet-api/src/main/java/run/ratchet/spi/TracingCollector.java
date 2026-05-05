package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.util.Map;
import java.util.UUID;

/**
 * Receives job execution lifecycle events for distributed tracing.
 *
 * <h2>Two-phase propagation</h2>
 *
 * <p>The submitting thread and the executing thread are decoupled. At enqueue time the caller's
 * active trace context is captured via {@link #captureCurrentContext()} and stored with the job.
 * When execution begins, the stored context is passed to {@link #jobExecutionStarted} so the
 * implementation can create a child span parented to the original caller's trace.
 *
 * <h2>Scope lifecycle</h2>
 *
 * <p>{@link #jobExecutionStarted} returns an {@link ExecutionScope} that the RI holds open for the
 * duration of one execution attempt. The scope is always closed in a {@code finally} block, so
 * implementations must tolerate being closed without a prior outcome call (treating it as an
 * abandoned/cancelled attempt).
 *
 * <h2>MDC integration</h2>
 *
 * <p>When a scope is active, implementations <em>must</em> inject {@code traceId} and {@code
 * spanId} into the logging MDC so that log lines emitted during job execution carry trace
 * coordinates alongside the four stable Ratchet MDC keys ({@code jobId}, {@code node}, {@code
 * jobCreator}, {@code jobType}). These keys must be removed when the scope is closed — use targeted
 * {@code MDC.remove(key)} calls, never {@code MDC.clear()}, to avoid wiping application MDC keys
 * set by Servlet filters or JAX-RS interceptors. Removal of the four Ratchet-owned keys remains the
 * responsibility of {@link run.ratchet.ri.core.JobMdcContext}.
 *
 * <h2>Default implementation</h2>
 *
 * <p>The default no-op implementation captures no context and returns a no-op scope. Opt in to real
 * tracing by placing {@code ratchet-micrometer} (Micrometer Tracing bridge) or {@code ratchet-otel}
 * (OpenTelemetry direct) on the deployment classpath.
 */
@Incubating
public interface TracingCollector {

  /**
   * Called at enqueue time on the submitting thread to capture the active trace context.
   *
   * <p>The returned map is a W3C TraceContext carrier (e.g. {@code traceparent} / {@code
   * tracestate} keys) or any implementation-defined propagation headers. It is stored with the job
   * and passed back verbatim to {@link #jobExecutionStarted} when execution begins.
   *
   * <p>Default: returns an empty map (no parent context captured; jobs start a new root trace).
   */
  default Map<String, String> captureCurrentContext() {
    return Map.of();
  }

  /**
   * Called at the start of a job execution attempt, on the executing thread.
   *
   * <p>The {@code parentContext} is the carrier map returned by {@link #captureCurrentContext()} at
   * enqueue time. Implementations should extract the parent span from this map and create a child
   * span that is active on the current thread for the duration of the returned scope.
   *
   * @param jobId unique job identifier
   * @param type public job type
   * @param priority job priority at the time of execution
   * @param parentContext propagation carrier captured at enqueue time; may be empty
   * @return a scope handle that the RI will close when this execution attempt ends
   */
  default ExecutionScope jobExecutionStarted(
      UUID jobId, JobType type, JobPriority priority, Map<String, String> parentContext) {
    return NoOpExecutionScope.INSTANCE;
  }

  /**
   * Called at the start of a job execution attempt with additional scheduler metadata to tag on the
   * span. Default delegates to the original four-argument method for source compatibility.
   */
  default ExecutionScope jobExecutionStarted(
      UUID jobId,
      JobType type,
      JobPriority priority,
      Map<String, String> parentContext,
      Map<String, String> attributes) {
    return jobExecutionStarted(jobId, type, priority, parentContext);
  }

  /**
   * Scope handle representing one active job execution attempt.
   *
   * <p>The RI closes this in a {@code finally} block regardless of outcome. Implementations must be
   * idempotent — {@link #close()} must be safe to call even after {@link #success} or {@link
   * #failure} has already been invoked (treating it as a no-op second close).
   */
  interface ExecutionScope extends AutoCloseable {

    /**
     * Records a successful outcome and closes this scope.
     *
     * @param executionTimeMs wall-clock duration of this attempt
     */
    void success(long executionTimeMs);

    /**
     * Records a failure outcome and closes this scope.
     *
     * @param cause the exception that caused the failure
     * @param attempt the 1-based attempt number including this failure
     */
    void failure(Throwable cause, int attempt);

    /**
     * Closes this scope without recording an outcome. Used for early exits (cancelled during
     * execution, circuit breaker open, permit denied). Safe to call after {@link #success} or
     * {@link #failure} — implementations must be idempotent.
     */
    @Override
    void close();
  }

  /** No-op scope returned by the default {@link #jobExecutionStarted} implementation. */
  enum NoOpExecutionScope implements ExecutionScope {
    INSTANCE;

    @Override
    public void success(long executionTimeMs) {}

    @Override
    public void failure(Throwable cause, int attempt) {}

    @Override
    public void close() {}
  }
}
