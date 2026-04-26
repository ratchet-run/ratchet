package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for {@link run.ratchet.api.JobSchedulerService#cancelJob(long) cancelJob}
 * semantics: cancellation must succeed for an in-flight or pending job and must be a no-op for a
 * job already in a terminal state.
 *
 * <p>The "cancel while running" case relies on a blocking task that the test releases through a
 * {@link CountDownLatch}. Implementations whose schedulers cannot interrupt running tasks may
 * choose to mark cancellation as terminal-on-completion instead — that is allowed; the contract
 * only requires that {@code awaitCancelled} eventually observes the CANCELLED event.
 */
public abstract class AbstractJobCancelContract {

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
  }

  @Test
  void cancelPendingJob_neverStarts() {
    // Submit a job with a delay long enough that cancel races ahead of execution.
    JobHandle handle = runtime().scheduler().schedule(Duration.ofSeconds(30), () -> {}).submit();

    boolean cancelled = runtime().scheduler().cancelJob(handle.id());
    assertTrue(cancelled, "cancelJob on a PENDING job should return true");

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "Cancelled pending job must surface a CANCELLED event");
    assertFalse(
        runtime().probe().awaitExecuted(handle, Duration.ofMillis(250)),
        "Cancelled pending job must never start executing");
  }

  @Test
  void cancelTerminalJob_returnsFalse() {
    JobHandle handle = runtime().scheduler().enqueueNow(() -> {});
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job must complete before terminal-cancel assertion");

    boolean cancelled = runtime().scheduler().cancelJob(handle.id());
    assertFalse(cancelled, "cancelJob on a SUCCEEDED job must return false");
  }

  @Test
  void cancelRunningJob_eventuallyCancels() throws InterruptedException {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(
                () -> {
                  started.countDown();
                  release.await();
                })
            .submit();

    assertTrue(
        started.await(defaultTimeout().toMillis(), TimeUnit.MILLISECONDS),
        "Job body must start before cancel attempt");

    runtime().scheduler().cancelJob(handle.id());
    release.countDown();

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "Running job cancellation must surface a CANCELLED event eventually");
  }
}
