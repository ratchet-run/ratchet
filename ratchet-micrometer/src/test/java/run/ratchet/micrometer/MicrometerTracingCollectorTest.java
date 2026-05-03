package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.TracingCollector.ExecutionScope;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import jakarta.enterprise.inject.Instance;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MicrometerTracingCollectorTest {

  @Mock private Tracer tracer;
  @Mock private Propagator propagator;
  @Mock private Span span;
  @Mock private Span.Builder spanBuilder;
  @Mock private Tracer.SpanInScope spanInScope;
  @Mock private Instance<Tracer> tracerInstance;
  @Mock private Instance<Propagator> propagatorInstance;

  private MicrometerTracingCollector collectorWithTracers() {
    when(tracerInstance.isResolvable()).thenReturn(true);
    when(tracerInstance.get()).thenReturn(tracer);
    when(propagatorInstance.isResolvable()).thenReturn(true);
    when(propagatorInstance.get()).thenReturn(propagator);
    return new MicrometerTracingCollector(tracerInstance, propagatorInstance);
  }

  private MicrometerTracingCollector nullCollector() {
    when(tracerInstance.isResolvable()).thenReturn(false);
    when(propagatorInstance.isResolvable()).thenReturn(false);
    return new MicrometerTracingCollector(tracerInstance, propagatorInstance);
  }

  // ── captureCurrentContext ─────────────────────────────────────────────────

  @Test
  void captureCurrentContext_nullTracer_returnsEmpty() {
    assertTrue(nullCollector().captureCurrentContext().isEmpty());
  }

  @Test
  void captureCurrentContext_noActiveSpan_returnsEmpty() {
    when(tracer.currentSpan()).thenReturn(null);

    assertTrue(collectorWithTracers().captureCurrentContext().isEmpty());
  }

  @Test
  void captureCurrentContext_noopSpan_returnsEmpty() {
    Span noopSpan = mock(Span.class);
    when(noopSpan.isNoop()).thenReturn(true);
    when(tracer.currentSpan()).thenReturn(noopSpan);

    assertTrue(collectorWithTracers().captureCurrentContext().isEmpty());
  }

  @Test
  void captureCurrentContext_activeSpan_injectsIntoCarrier() {
    TraceContext traceCtx = mock(TraceContext.class);
    when(span.isNoop()).thenReturn(false);
    when(span.context()).thenReturn(traceCtx);
    when(tracer.currentSpan()).thenReturn(span);
    // inject() is void — use doAnswer
    doAnswer(
            inv -> {
              Map<String, String> carrier = inv.getArgument(1);
              carrier.put("traceparent", "00-abc-def-01");
              return null;
            })
        .when(propagator)
        .inject(eq(traceCtx), any(Map.class), any());

    Map<String, String> ctx = collectorWithTracers().captureCurrentContext();

    assertEquals("00-abc-def-01", ctx.get("traceparent"));
  }

  // ── jobExecutionStarted ───────────────────────────────────────────────────

  @Test
  void jobExecutionStarted_nullTracer_returnsNoOpScope() {
    ExecutionScope scope =
        nullCollector()
            .jobExecutionStarted(UUID.randomUUID(), JobType.SINGLE, JobPriority.NORMAL, Map.of());

    assertNotNull(scope);
    assertDoesNotThrow(() -> scope.success(100));
    assertDoesNotThrow(() -> scope.failure(new RuntimeException(), 1));
    assertDoesNotThrow(scope::close);
  }

  @Test
  void jobExecutionStarted_emptyParentContext_startsNewSpan() {
    // tracer.nextSpan() returns Span (Micrometer's Span is builder+started in one type)
    when(tracer.nextSpan()).thenReturn(span);
    when(span.start()).thenReturn(span);
    when(span.name(any())).thenReturn(span);
    when(span.tag(any(), any())).thenReturn(span);
    when(tracer.withSpan(span)).thenReturn(spanInScope);

    collectorWithTracers()
        .jobExecutionStarted(UUID.randomUUID(), JobType.SINGLE, JobPriority.NORMAL, Map.of());

    verify(tracer).nextSpan();
  }

  @Test
  void jobExecutionStarted_withParentContext_extractsFromCarrier() {
    Map<String, String> parentCtx = Map.of("traceparent", "00-parent-01");
    // propagator.extract() returns Span.Builder
    when(propagator.extract(eq(parentCtx), any())).thenReturn(spanBuilder);
    when(spanBuilder.start()).thenReturn(span);
    when(span.name(any())).thenReturn(span);
    when(span.tag(any(), any())).thenReturn(span);
    when(tracer.withSpan(span)).thenReturn(spanInScope);

    collectorWithTracers()
        .jobExecutionStarted(UUID.randomUUID(), JobType.SINGLE, JobPriority.HIGH, parentCtx);

    verify(propagator).extract(eq(parentCtx), any());
  }

  // ── ExecutionScope outcome tagging ────────────────────────────────────────

  private ExecutionScope startedScope() {
    when(tracer.nextSpan()).thenReturn(span);
    when(span.start()).thenReturn(span);
    when(span.name(any())).thenReturn(span);
    when(span.tag(any(), any())).thenReturn(span);
    when(tracer.withSpan(span)).thenReturn(spanInScope);
    return collectorWithTracers()
        .jobExecutionStarted(UUID.randomUUID(), JobType.SINGLE, JobPriority.NORMAL, Map.of());
  }

  @Test
  void executionScope_success_tagsOutcomeAndEndsSpan() {
    startedScope().success(250);

    verify(span).tag("ratchet.outcome", "success");
    verify(span).end();
    verify(spanInScope).close();
  }

  @Test
  void executionScope_failure_tagsOutcomeAttemptAndError() {
    RuntimeException cause = new RuntimeException("boom");

    startedScope().failure(cause, 2);

    verify(span).tag("ratchet.outcome", "failure");
    verify(span).tag("ratchet.attempt", "2");
    verify(span).error(cause);
    verify(span).end();
  }

  @Test
  void executionScope_close_tagsAbandoned() {
    startedScope().close();

    verify(span).tag("ratchet.outcome", "abandoned");
    verify(span).end();
  }

  @Test
  void executionScope_doubleClose_isIdempotent() {
    ExecutionScope scope = startedScope();
    scope.success(100);
    scope.close(); // second close must not call end() again

    verify(span).end(); // exactly once, guarded by AtomicBoolean
  }
}
