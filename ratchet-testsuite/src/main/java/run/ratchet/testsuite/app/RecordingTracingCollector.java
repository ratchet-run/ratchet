package run.ratchet.testsuite.app;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.TracingCollector;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-double {@link TracingCollector} that records enqueue-time context captures and execution
 * scope invocations. Allows integration tests to verify the two-phase trace propagation round-trip
 * without a real tracing backend.
 *
 * <p>Call {@link #setContextToCapture(Map)} before enqueuing a job to inject a fake W3C carrier
 * map. The map returned by {@link #captureCurrentContext()} is recorded and later passed to {@link
 * #jobExecutionStarted} — tests can assert the round-trip via {@link #getReceivedParentContexts()}.
 *
 * <p>Used by {@code TracingPropagationIT}.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class RecordingTracingCollector implements TracingCollector {

  private static volatile Map<String, String> contextToCapture = Map.of();
  private static final List<Map<String, String>> receivedParentContexts =
      new CopyOnWriteArrayList<>();

  /** Injects the carrier map that {@link #captureCurrentContext()} will return. */
  public static void setContextToCapture(Map<String, String> context) {
    contextToCapture = Map.copyOf(context);
  }

  /**
   * Returns an unmodifiable view of the {@code parentContext} maps received by {@link
   * #jobExecutionStarted} in call order. One entry per execution attempt.
   */
  public static List<Map<String, String>> getReceivedParentContexts() {
    return Collections.unmodifiableList(receivedParentContexts);
  }

  public static void reset() {
    contextToCapture = Map.of();
    receivedParentContexts.clear();
  }

  @Override
  public Map<String, String> captureCurrentContext() {
    return contextToCapture;
  }

  @Override
  public ExecutionScope jobExecutionStarted(
      UUID jobId, JobType type, JobPriority priority, Map<String, String> parentContext) {
    receivedParentContexts.add(Map.copyOf(parentContext));
    return NoOpExecutionScope.INSTANCE;
  }
}
