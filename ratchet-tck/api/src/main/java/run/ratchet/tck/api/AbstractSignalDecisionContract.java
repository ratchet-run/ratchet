package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.SignalDecision;

/**
 * Base contract for signal-waiting jobs with structured approval/rejection decisions.
 *
 * <p>A delivered {@link SignalDecision} unblocks the job from WAITING to PENDING regardless of
 * outcome. The job body then applies domain-specific behavior by reading the decision from {@code
 * JobContext.signalPayload(SignalDecision.class)}.
 */
public abstract class AbstractSignalDecisionContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void signalWaitingJobDoesNotExecuteUntilDecisionIsDelivered() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordSignalDecision)
            .awaitSignal("approval-tck-approve", defaultTimeout())
            .submit();
    runtime().probe().track(handle);

    assertFalse(
        runtime().probe().awaitExecuted(handle, quietWindow()),
        "Signal-waiting job must not execute before a signal decision is delivered");

    assertEquals(
        1, runtime().scheduler().deliverSignal(handle.id(), SignalDecision.approved("ok")));

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Approved signal decision should unblock and complete the job");
    assertEquals(List.of("APPROVED:ok:null"), TckJobs.signalDecisions());
  }

  @Test
  void rejectedDecisionStillUnblocksAndIsVisibleToJobBody() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordSignalDecision)
            .awaitSignal("approval-tck-reject", defaultTimeout())
            .submit();
    runtime().probe().track(handle);

    assertEquals(
        1,
        runtime()
            .scheduler()
            .deliverSignal(handle.id(), SignalDecision.rejected("needs-review", "denied")));

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Rejected signal decision should unblock and complete the job");
    assertEquals(List.of("REJECTED:needs-review:denied"), TckJobs.signalDecisions());
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  // 1 s comfortably exceeds a typical poll cycle so spurious early execution is reliably caught
  // on loaded CI runners, while 300 ms was narrow enough to produce false passes under load.
  protected Duration quietWindow() {
    return Duration.ofMillis(1000);
  }
}
