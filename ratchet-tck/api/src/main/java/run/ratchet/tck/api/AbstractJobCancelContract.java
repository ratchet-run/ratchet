package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for {@link run.ratchet.api.JobSchedulerService#cancelJob(long) cancelJob}
 * semantics: cancellation must succeed for an in-flight or pending job and must be a no-op for a
 * job already in a terminal state.
 *
 * <p>The "cancel while running" case relies on {@link TckJobs#blockUntilReleased()} as the task
 * body. Implementations whose schedulers cannot interrupt running tasks may choose to mark
 * cancellation as terminal-on-completion instead — that is allowed; the contract only requires that
 * {@code awaitCancelled} eventually observes the CANCELLED event.
 */
public abstract class AbstractJobCancelContract {

  // 1 s gives the scheduler one full poll cycle of headroom on loaded CI runners before we conclude
  // the chain child was suppressed. 500 ms was tight enough to produce false passes when the child
  // was queued but not yet dispatched within the observation window.
  private static final Duration CHAIN_CHILD_QUIET_WINDOW = Duration.ofMillis(1000);
  private static final Duration CHAIN_CHILD_POLL_INTERVAL = Duration.ofMillis(50);

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void cancelPendingJob_neverStarts() {
    // Submit a job with a delay long enough that cancel races ahead of execution.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(30), TckJobs::noop).submit();
    runtime().probe().track(handle);

    boolean cancelled = runtime().scheduler().cancelJob(handle.id());
    assertTrue(cancelled, "cancelJob on a PENDING job should return true");

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "Cancelled pending job must surface a CANCELLED event — the API javadoc says PENDING "
            + "jobs 'transition directly to CANCELED', and a state transition that's invisible "
            + "to listeners breaks downstream consumers (audit logs, observers, monitoring).");
    assertFalse(
        runtime().probe().awaitExecuted(handle, Duration.ofMillis(250)),
        "Cancelled pending job must never start executing");
  }

  @Test
  void cancelTerminalJob_returnsFalse() {
    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job must complete before terminal-cancel assertion");

    boolean cancelled = runtime().scheduler().cancelJob(handle.id());
    assertFalse(cancelled, "cancelJob on a SUCCEEDED job must return false");
  }

  @Test
  void cancelRunningJob_eventuallyCancels() throws InterruptedException {
    CountDownLatch started = TckJobs.beginBlocking();

    JobHandle handle = runtime().scheduler().enqueue(TckJobs::blockUntilReleased).submit();
    runtime().probe().track(handle);

    assertTrue(
        started.await(defaultTimeout().toMillis(), TimeUnit.MILLISECONDS),
        "Job body must start before cancel attempt");

    runtime().scheduler().cancelJob(handle.id());
    TckJobs.release();

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "Running job cancellation must surface a CANCELLED event eventually");
  }

  @Test
  void cancelChainParent_preventsChainChildExecution() throws InterruptedException {
    // Schedule a chain with a 30-second delay on the parent so it stays PENDING long enough
    // to cancel. The child (recordStepA) must never execute.
    JobHandle handle =
        runtime()
            .scheduler()
            .schedule(Duration.ofSeconds(30), TckJobs::noop)
            .then(TckJobs::recordStepA)
            .submit();
    runtime().probe().track(handle);

    boolean cancelled = runtime().scheduler().cancelJob(handle.id());
    assertTrue(cancelled, "cancelJob on a PENDING chain parent should return true");

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "Cancelled chain parent must surface a CANCELLED event");

    assertNoChainEventsWithin(CHAIN_CHILD_QUIET_WINDOW);
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }

  @SuppressWarnings("BusyWait")
  private void assertNoChainEventsWithin(Duration quietWindow) throws InterruptedException {
    // The API does not expose handles for child chain jobs. After the parent cancellation event is
    // visible, observe a short, bounded quiet window and fail fast if a child records execution.
    // No event is available to wait on, so polling at CHAIN_CHILD_POLL_INTERVAL is intentional.
    long deadlineNanos = System.nanoTime() + quietWindow.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      assertTrue(
          TckJobs.chainEvents().isEmpty(),
          "Chain child must not execute when the parent job was cancelled");
      Thread.sleep(CHAIN_CHILD_POLL_INTERVAL.toMillis());
    }
    assertTrue(
        TckJobs.chainEvents().isEmpty(),
        "Chain child must not execute when the parent job was cancelled");
  }
}
