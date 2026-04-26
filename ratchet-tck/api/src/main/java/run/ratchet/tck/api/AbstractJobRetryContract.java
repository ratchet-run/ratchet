package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for retry semantics on {@link run.ratchet.api.JobBuilder#withMaxRetries
 * withMaxRetries}.
 *
 * <p>The contract requires that retries cap at {@code maxRetries} and that the task body is invoked
 * exactly {@code maxRetries + 1} times for a deterministically-failing task. Backoff timing is
 * intentionally NOT asserted here — wall-clock timing assertions are flaky on poll-based
 * schedulers; clock-driven assertions belong in {@link AbstractDelayedSchedulingContract}.
 */
public abstract class AbstractJobRetryContract {

  protected abstract RatchetTckRuntime runtime();

  /**
   * Default timeout. Generous because retries serialize through scheduler polls; subclasses may
   * shrink for fast in-memory implementations.
   */
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void alwaysFailingTask_invokesTaskMaxRetriesPlusOneTimesThenFails() {
    int maxRetries = 2;

    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::throwIntentional)
            .withMaxRetries(maxRetries)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitFailed(handle, defaultTimeout()),
        "Task that exhausts retries must reach FAILED");

    assertEquals(
        maxRetries + 1,
        runtime().probe().invocationCount(handle),
        "Always-failing task must be invoked exactly maxRetries+1 times before terminal FAILED");
  }

  @Test
  void zeroMaxRetries_invokesTaskOnceThenFails() {
    JobHandle handle =
        runtime().scheduler().enqueue(TckJobs::throwIntentional).withMaxRetries(0).submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitFailed(handle, defaultTimeout()),
        "Zero-retries failing task must reach FAILED");
    assertEquals(
        1,
        runtime().probe().invocationCount(handle),
        "withMaxRetries(0) must invoke task body exactly once");
  }
}
