package run.ratchet.micrometer;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.TracingCollector;

/**
 * Micrometer Tracing-backed {@link TracingCollector}.
 *
 * <p>Creates one span per job execution attempt. The span name is {@code ratchet.job} and carries
 * the following tags:
 *
 * <ul>
 *   <li>{@code ratchet.job.id} — UUID of the job
 *   <li>{@code ratchet.job.type} — public job type name
 *   <li>{@code ratchet.job.priority} — job priority at execution time
 *   <li>{@code ratchet.outcome} — {@code success}, {@code failure}, or {@code abandoned}
 *   <li>{@code ratchet.attempt} — attempt number (failure path only)
 * </ul>
 *
 * <p>W3C {@code traceparent} context captured at enqueue time is restored as the parent span when
 * provided.
 *
 * <p>{@link Tracer} and {@link Propagator} are injected optionally. If no tracing bridge is on the
 * deployment classpath (e.g. {@code micrometer-tracing-bridge-otel}), this collector degrades
 * silently to no-op behaviour rather than failing at deployment.
 *
 * <p>MDC keys {@code traceId} and {@code spanId} are populated automatically by the active
 * Micrometer Tracing bridge when the span scope is open, and removed when the scope is closed.
 */
@Alternative
@Priority(1000)
@ApplicationScoped
public class MicrometerTracingCollector implements TracingCollector {

  private final Tracer tracer;
  private final Propagator propagator;

  protected MicrometerTracingCollector() {
    this.tracer = null;
    this.propagator = null;
  }

  @Inject
  public MicrometerTracingCollector(
      Instance<Tracer> tracerInstance, Instance<Propagator> propagatorInstance) {
    this.tracer = tracerInstance.isResolvable() ? tracerInstance.get() : null;
    this.propagator = propagatorInstance.isResolvable() ? propagatorInstance.get() : null;
  }

  @Override
  public Map<String, String> captureCurrentContext() {
    if (tracer == null || propagator == null) {
      return Map.of();
    }
    Span current = tracer.currentSpan();
    if (current == null || current.isNoop()) {
      return Map.of();
    }
    Map<String, String> carrier = new LinkedHashMap<>();
    propagator.inject(current.context(), carrier, Map::put);
    return Map.copyOf(carrier);
  }

  @Override
  public ExecutionScope jobExecutionStarted(
      UUID jobId, JobType type, JobPriority priority, Map<String, String> parentContext) {
    return jobExecutionStarted(jobId, type, priority, parentContext, Map.of());
  }

  @Override
  public ExecutionScope jobExecutionStarted(
      UUID jobId,
      JobType type,
      JobPriority priority,
      Map<String, String> parentContext,
      Map<String, String> attributes) {
    if (tracer == null || propagator == null) {
      return NoOpExecutionScope.INSTANCE;
    }
    // tracer.nextSpan() returns Span; propagator.extract() returns Span.Builder —
    // start both into Span then apply tags on the unified type.
    Span span =
        parentContext.isEmpty()
            ? tracer.nextSpan().start()
            : propagator.extract(parentContext, Map::get).start();
    span.name("ratchet.job")
        .tag("ratchet.job.id", jobId.toString())
        .tag("ratchet.job.type", type.name())
        .tag("ratchet.job.priority", priority.name());
    if (attributes != null) {
      attributes.forEach(span::tag);
    }

    return new MicrometerExecutionScope(span, tracer.withSpan(span));
  }

  private static final class MicrometerExecutionScope implements ExecutionScope {

    private final Span span;
    private final Tracer.SpanInScope spanInScope;
    private final AtomicBoolean closed = new AtomicBoolean();

    MicrometerExecutionScope(Span span, Tracer.SpanInScope spanInScope) {
      this.span = span;
      this.spanInScope = spanInScope;
    }

    @Override
    public void success(long executionTimeMs) {
      if (closed.compareAndSet(false, true)) {
        span.tag("ratchet.outcome", "success");
        end();
      }
    }

    @Override
    public void failure(Throwable cause, int attempt) {
      if (closed.compareAndSet(false, true)) {
        span.tag("ratchet.outcome", "failure")
            .tag("ratchet.attempt", String.valueOf(attempt))
            .error(cause);
        end();
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        span.tag("ratchet.outcome", "abandoned");
        end();
      }
    }

    private void end() {
      try {
        spanInScope.close();
      } finally {
        span.end();
      }
    }
  }
}
