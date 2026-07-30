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
package run.ratchet.tck.api.transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * TCK contract: mutation operations on {@link JobSchedulerService} documented as transaction
 * attribute {@code REQUIRED} MUST participate in the caller's transaction.
 *
 * <ul>
 *   <li>When the surrounding transaction commits, the mutation MUST be durably visible.
 *   <li>When the surrounding transaction rolls back, the mutation MUST be invisible — the entity
 *       must be in the same state as before the call.
 * </ul>
 *
 * <p>Covers {@code cancelJob} and {@code pauseJob}. The {@code enqueueNow} case is covered by
 * {@link AbstractTxEnqueueContract}. Resume and retry are omitted here because they require
 * store-specific preconditions (PAUSED / FAILED state) that cannot be set up generically without
 * additional TCK infrastructure.
 *
 * <p>Implementations backed by a non-transactional store return {@code false} from {@link
 * RatchetTckRuntime#supportsCallerTransactionRollback()}. Only the rollback contracts then report
 * {@code N/A}; commit-visible contracts still run and must pass. Do not disable the whole subclass,
 * because that would suppress assertions a non-transactional store is still expected to satisfy.
 */
public abstract class AbstractTxRequiredContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void cancelJob_commit_isVisible() throws Exception {
    // 30 s delay keeps the job PENDING throughout the test — no race with the scheduler.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(30), TckJobs::noop).submit();
    runtime().probe().track(handle);

    transactionDriver()
        .committing(
            () -> {
              runtime().scheduler().cancelJob(handle.id());
            });

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "cancelJob inside a committed TX must transition the job to CANCELLED and publish a "
            + "CANCELLED event. If this fails the implementation did not persist the cancellation "
            + "within the TX.");
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Such an override must delegate to {@code super} and must
   *     not replace the assertion body.
   */
  @Test
  protected void cancelJob_rollback_isNotVisible() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(30), TckJobs::noop).submit();
    runtime().probe().track(handle);

    transactionDriver()
        .rollingBack(
            () -> {
              runtime().scheduler().cancelJob(handle.id());
            });

    assertFalse(
        runtime().probe().awaitCancelled(handle, quietWindow()),
        "cancelJob inside a rolled-back TX must not cancel the job. The PENDING state must be "
            + "restored as if the call never happened. A CANCELLED event here means the "
            + "implementation wrote outside the TX boundary.");
  }

  @Test
  void pauseJob_commit_suppressesExecution() throws Exception {
    // 30 s delay: job is PENDING well past the quiet window below, so the only way it can
    // execute is if the pause was not applied.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(30), TckJobs::noop).submit();
    runtime().probe().track(handle);

    transactionDriver()
        .committing(
            () -> {
              runtime().scheduler().pauseJob(handle.id());
            });

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "pauseJob inside a committed TX must prevent the job from executing. A COMPLETED event "
            + "here means the pause was not persisted.");
  }

  protected abstract RatchetTckRuntime runtime();

  protected abstract RatchetTransactionDriver transactionDriver();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  /**
   * Quiet window used for negative assertions: long enough to catch a spurious transition if the
   * implementation is broken, short enough that a passing test suite does not drag.
   */
  protected Duration quietWindow() {
    return Duration.ofMillis(750);
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Capability-based skip overrides must delegate to {@code
   *     super} and must not replace the assertion body. A runner may instead keep an inherited
   *     method disabled when the runner itself hangs before the contract can complete; such a
   *     sentinel override must fail loudly if its disable annotation is removed.
   */
  @Test
  protected void pauseJob_rollback_doesNotSuppressExecution() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");
    // 2 s delay: comfortably exceeds a typical poll cycle so the begin → pause → rollback
    // sequence completes before the scheduler can claim the job. The old 500 ms budget was
    // narrower than a single poll interval on loaded CI runners, causing vacuous passes.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(2), TckJobs::noop).submit();
    runtime().probe().track(handle);

    transactionDriver()
        .rollingBack(
            () -> {
              runtime().scheduler().pauseJob(handle.id());
            });

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "pauseJob inside a rolled-back TX must not suppress execution. The job must run normally "
            + "once its delay expires. Timeout here means the implementation applied the pause "
            + "outside the TX boundary.");
  }
}
