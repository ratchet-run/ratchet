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
package run.ratchet.tck.jakarta;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * TCK contract: mutation operations on {@link run.ratchet.api.JobSchedulerService} documented as
 * transaction attribute {@code REQUIRED} MUST participate in the caller's JTA transaction.
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
 * <p>Implementations backed by a non-JTA store (e.g. MongoDB) skip per method, not per class: the
 * two rollback contracts self-skip via {@code assumeTrue} on the {@code ratchet.test.db.type}
 * property (a non-JTA store cannot undo the write on {@code rollback}), while the commit-visible
 * contracts still run and must pass. Do not {@code @Disabled} the whole subclass — that would
 * suppress the commit assertions that a non-JTA store is expected to satisfy.
 */
public abstract class AbstractTxRequiredContract {

  @Inject protected UserTransaction tx;

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

    tx.begin();
    runtime().scheduler().cancelJob(handle.id());
    tx.commit();

    assertTrue(
        runtime().probe().awaitCancelled(handle, defaultTimeout()),
        "cancelJob inside a committed TX must transition the job to CANCELLED and publish a "
            + "CANCELLED event. If this fails the implementation did not persist the cancellation "
            + "within the TX.");
  }

  /**
   * @apiNote Intentionally {@code protected} so runtime-specific TCK subclasses can override this
   *     test to attach deployment-specific annotations. Overriders MUST delegate to {@code super};
   *     replacing the body silently suppresses the contract.
   */
  @Test
  protected void cancelJob_rollback_isNotVisible() throws Exception {
    assumeTrue(
        !"mongodb".equals(System.getProperty("ratchet.test.db.type", "")),
        "MongoDB does not participate in JTA rollback");
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(30), TckJobs::noop).submit();
    runtime().probe().track(handle);

    tx.begin();
    runtime().scheduler().cancelJob(handle.id());
    tx.rollback();

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

    tx.begin();
    runtime().scheduler().pauseJob(handle.id());
    tx.commit();

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "pauseJob inside a committed TX must prevent the job from executing. A COMPLETED event "
            + "here means the pause was not persisted.");
  }

  protected abstract RatchetTckRuntime runtime();

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
   * @apiNote Intentionally {@code protected} so runtime-specific TCK subclasses (e.g. the RI
   *     test-suite under {@code ratchet-testsuite}) can override this test to attach
   *     deployment-specific annotations such as {@code @DisabledIfSystemProperty}. Overriders MUST
   *     delegate to {@code super}; replacing the body silently suppresses the contract.
   */
  @Test
  protected void pauseJob_rollback_doesNotSuppressExecution() throws Exception {
    assumeTrue(
        !"mongodb".equals(System.getProperty("ratchet.test.db.type", "")),
        "MongoDB does not participate in JTA rollback");
    // 2 s delay: comfortably exceeds a typical poll cycle so the begin → pause → rollback
    // sequence completes before the scheduler can claim the job. The old 500 ms budget was
    // narrower than a single poll interval on loaded CI runners, causing vacuous passes.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofSeconds(2), TckJobs::noop).submit();
    runtime().probe().track(handle);

    tx.begin();
    runtime().scheduler().pauseJob(handle.id());
    tx.rollback();

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "pauseJob inside a rolled-back TX must not suppress execution. The job must run normally "
            + "once its delay expires. Timeout here means the implementation applied the pause "
            + "outside the TX boundary.");
  }
}
