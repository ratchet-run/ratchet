package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for the happy-path lifecycle of a submitted job: submit → started → completed.
 *
 * <p>Subclasses provide a {@link RatchetTckRuntime} via {@link #runtime()} and may override {@link
 * #defaultTimeout()} for slow runtimes (containers booting JTA, etc.).
 */
public abstract class AbstractJobLifecycleContract {

  /** The runtime under test. Same instance is returned across calls within a single test. */
  protected abstract RatchetTckRuntime runtime();

  /**
   * Default timeout for {@code await*} probe assertions. Override for runtimes that need more
   * generous bounds (e.g. JTA-backed schedulers in a managed container).
   */
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
  }

  @Test
  void submit_thenStartsAndCompletes() {
    JobHandle handle = runtime().scheduler().enqueueNow(() -> {});
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Submitted no-op job should complete within timeout");

    List<ProbeEvent> events = runtime().probe().events(handle);
    assertTrue(
        events.stream().anyMatch(e -> e.type() == ProbeEvent.Type.STARTED),
        "Lifecycle must include STARTED before COMPLETED. Observed: " + events);
    assertEquals(
        1,
        runtime().probe().invocationCount(handle),
        "Successful no-op job must invoke task body exactly once");
  }

  @Test
  void submit_failingTaskTransitionsToFailed() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(
                () -> {
                  throw new IllegalStateException("intentional");
                })
            .withMaxRetries(0)
            .submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitFailed(handle, defaultTimeout()),
        "Failing job with no retries must reach FAILED within timeout");
  }
}
