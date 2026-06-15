/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
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
 * OpenTelemetry-backed {@link TracingCollector}.
 *
 * <p>Creates one span per job execution attempt using the OTel API directly, without the Micrometer
 * Tracing layer. The span name is {@code ratchet.job} and carries the following attributes:
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
 * <p>{@link OpenTelemetry} is injected via CDI {@link Instance} if a bean is available; otherwise
 * falls back to {@link GlobalOpenTelemetry#get()}, which allows framework integrations (Quarkus,
 * Spring, etc.) that configure the global instance to work without explicit CDI wiring.
 *
 * <p>MDC keys {@code traceId} and {@code spanId} are populated by the OTel SDK's MDC context
 * storage provider when a logging bridge (e.g. {@code opentelemetry-logback-appender} or {@code
 * opentelemetry-log4j2-appender}) is on the classpath.
 */
@Alternative
@Priority(1000)
@ApplicationScoped
public class OtelTracingCollector implements TracingCollector {

  private final OpenTelemetry openTelemetry;

  /** CDI proxy constructor — package-private to keep it out of the public API surface. */
  OtelTracingCollector() {
    this.openTelemetry = null;
  }

  @Inject
  public OtelTracingCollector(Instance<OpenTelemetry> openTelemetryInstance) {
    this.openTelemetry =
        openTelemetryInstance.isResolvable()
            ? openTelemetryInstance.get()
            : GlobalOpenTelemetry.get();
  }

  @Override
  public Map<String, String> captureCurrentContext() {
    if (openTelemetry == null) {
      return Map.of();
    }
    Span current = Span.current();
    if (!current.getSpanContext().isValid()) {
      return Map.of();
    }
    Map<String, String> carrier = new LinkedHashMap<>();
    openTelemetry
        .getPropagators()
        .getTextMapPropagator()
        .inject(
            Context.current(),
            carrier,
            (map, key, value) -> {
              if (map != null && key != null && value != null) {
                map.put(key, value);
              }
            });
    return Map.copyOf(carrier);
  }

  @Override
  public ExecutionScope jobExecutionStarted(
      UUID jobId, JobType type, JobPriority priority, Map<String, String> parentContextMap) {
    return jobExecutionStarted(jobId, type, priority, parentContextMap, Map.of());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Entries in {@code attributes} are attached to the span as additional OTel attributes
   * verbatim. The RI publishes scheduler-side metadata keys here — notably {@code ratchet.node}
   * (originating node identity), {@code ratchet.tag.affinity} (worker-tag affinity match), and
   * {@code ratchet.queue.depth} (queue depth at dispatch time). Unknown keys are passed through
   * unchanged so the framework can extend the published metadata without requiring a collector
   * change.
   */
  @Override
  public ExecutionScope jobExecutionStarted(
      UUID jobId,
      JobType type,
      JobPriority priority,
      Map<String, String> parentContextMap,
      Map<String, String> attributes) {
    if (openTelemetry == null) {
      return NoOpExecutionScope.INSTANCE;
    }
    TextMapPropagator propagator = openTelemetry.getPropagators().getTextMapPropagator();
    Context otelParent =
        parentContextMap.isEmpty()
            ? Context.current()
            : propagator.extract(Context.current(), parentContextMap, MapGetter.INSTANCE);

    var spanBuilder =
        openTelemetry
            .getTracer("ratchet")
            .spanBuilder("ratchet.job")
            .setParent(otelParent)
            .setAttribute("ratchet.job.id", jobId.toString())
            .setAttribute("ratchet.job.type", type.name())
            .setAttribute("ratchet.job.priority", priority.name());
    if (attributes != null) {
      attributes.forEach(spanBuilder::setAttribute);
    }
    Span span = spanBuilder.startSpan();

    return new OtelExecutionScope(span, span.makeCurrent());
  }

  private enum MapGetter implements TextMapGetter<Map<String, String>> {
    INSTANCE;

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
      return carrier.keySet();
    }

    @Override
    public String get(Map<String, String> carrier, String key) {
      return carrier != null ? carrier.get(key) : null;
    }
  }

  private static final class OtelExecutionScope implements ExecutionScope {

    private final Span span;
    private final Scope scope;
    private final AtomicBoolean closed = new AtomicBoolean();

    OtelExecutionScope(Span span, Scope scope) {
      this.span = span;
      this.scope = scope;
    }

    @Override
    public void success(long executionTimeMs) {
      if (closed.compareAndSet(false, true)) {
        span.setStatus(StatusCode.OK).setAttribute("ratchet.outcome", "success");
        end();
      }
    }

    @Override
    public void failure(Throwable cause, int attempt) {
      if (closed.compareAndSet(false, true)) {
        span.setStatus(StatusCode.ERROR)
            .recordException(cause)
            .setAttribute("ratchet.outcome", "failure")
            .setAttribute("ratchet.attempt", attempt);
        end();
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        span.setAttribute("ratchet.outcome", "abandoned");
        end();
      }
    }

    private void end() {
      try {
        scope.close();
      } finally {
        span.end();
      }
    }
  }
}
