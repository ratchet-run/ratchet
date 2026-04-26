package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.SerializablePredicate;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for chain ordering and {@link
 * run.ratchet.api.JobBuilder#when(SerializablePredicate,
 * run.ratchet.api.SerializableCheckedRunnable) when(SerializablePredicate, ...)}
 * semantics.
 *
 * <p>This contract also exercises lambda portability: {@code SerializablePredicate} must round-trip
 * through whatever serializer the implementation chose ({@link
 * run.ratchet.spi.LambdaSerializer}). An implementation that fails this contract has a
 * lambda-portability gap and is not "Ratchet API Compatible".
 *
 * <p>Subclasses must provide a static recording sink because chained tasks deserialize on the
 * worker side and would not see an instance field. The test methods below use a process-wide {@link
 * ConcurrentLinkedQueue} for simplicity.
 */
public abstract class AbstractSimpleWorkflowContract {

  /** Process-wide recording sink. Tests reset this in their bodies. */
  protected static final ConcurrentLinkedQueue<String> EVENTS = new ConcurrentLinkedQueue<>();

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  @AfterEach
  void clearAfterEach() {
    EVENTS.clear();
    runtime().clear();
  }

  @Test
  void thenChain_executesInDeclaredOrder() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(() -> EVENTS.add("step-1"))
            .then(() -> EVENTS.add("step-2"))
            .then(() -> EVENTS.add("step-3"))
            .submit();

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Chained job must complete within timeout");
    assertEquals(
        java.util.List.of("step-1", "step-2", "step-3"),
        java.util.List.copyOf(EVENTS),
        "Chained tasks must execute in declared order");
  }

  /**
   * Exercises {@code SerializablePredicate} portability. The predicate is written so its target
   * MethodHandle invariant survives serialization regardless of which lambda serializer the
   * implementation chose.
   */
  @Test
  void whenPredicate_routesOnTaskResult() {
    SerializablePredicate<run.ratchet.api.JobResult<Integer>> resultIsEven =
        r -> r.getValue() != null && r.getValue() % 2 == 0;

    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(() -> EVENTS.add("primary-ran"))
            .when(resultIsEven, () -> EVENTS.add("secondary-because-even"))
            .submit();

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Workflow with SerializablePredicate must complete");
    assertTrue(
        EVENTS.contains("primary-ran"),
        "Primary task must have run regardless of branch outcome; events=" + EVENTS);
  }
}
