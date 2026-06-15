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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
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

  @Test
  void deliverDecisionToUnknownJobReturnsZero() {
    assertEquals(
        0,
        runtime().scheduler().deliverSignal(new UUID(0L, 1L), SignalDecision.approved("ignored")),
        "Delivering a decision to an unknown job id must return 0");
  }

  @Test
  void deliverDecisionToAlreadyCompletedJobReturnsZeroWithoutReinvoking() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordSignalDecision)
            .awaitSignal("approval-tck-idempotent", defaultTimeout())
            .submit();
    runtime().probe().track(handle);

    // First delivery unblocks the WAITING job and lets it run to completion.
    assertEquals(
        1, runtime().scheduler().deliverSignal(handle.id(), SignalDecision.approved("first")));
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "First decision should unblock and complete the job");

    // The job is now terminal. A second delivery must be a no-op per the documented idempotency
    // contract: return 0 and do not re-invoke the job body.
    assertEquals(
        0,
        runtime().scheduler().deliverSignal(handle.id(), SignalDecision.approved("second")),
        "Delivering to a non-WAITING (terminal) job must return 0 without modifying it");
    assertEquals(
        List.of("APPROVED:first:null"),
        TckJobs.signalDecisions(),
        "The second delivery must not re-invoke the job body");
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
