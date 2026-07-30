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
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * Base contract: a Ratchet runtime backed by a transaction-aware store MUST honor the caller's
 * transaction context on enqueue. Specifically:
 *
 * <ul>
 *   <li>An enqueue performed inside a committed transaction MUST result in the job being executed
 *       by the scheduler.
 *   <li>An enqueue performed inside a rolled-back transaction MUST NOT result in the job being
 *       executed.
 * </ul>
 *
 * <p>This contract is a conformance grade, not a universal hard requirement. A runtime whose store
 * does not participate in caller transactions returns {@code false} from {@link
 * RatchetTckRuntime#supportsCallerTransactionRollback()}. Its rollback-only case then reports
 * {@code N/A}; the commit-visible case still runs and must pass.
 */
public abstract class AbstractTxEnqueueContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void commitPublishesEnqueuedJob() throws Exception {
    JobHandle handle =
        transactionDriver()
            .committing(
                () -> {
                  JobHandle submitted = runtime().scheduler().enqueueNow(TckJobs::noop);
                  runtime().probe().track(submitted);
                  return submitted;
                });

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job enqueued in a committed transaction must execute. If this fails on a "
            + "transaction-aware store, the runtime is publishing to its scheduler queue eagerly "
            + "instead of deferring to commit, breaking caller atomicity.");
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Such an override must delegate to {@code super} and must
   *     not replace the assertion body.
   */
  @Test
  protected void rollbackSuppressesEnqueuedJob() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");
    JobHandle handle =
        transactionDriver()
            .rollingBack(
                () -> {
                  JobHandle submitted = runtime().scheduler().enqueueNow(TckJobs::noop);
                  runtime().probe().track(submitted);
                  return submitted;
                });

    assertFalse(
        runtime().probe().awaitCompleted(handle, rollbackQuietWindow()),
        "Job enqueued in a rolled-back transaction must NOT execute. A COMPLETED event for "
            + "handle "
            + handle.id()
            + " indicates the runtime ignored the rollback — caller atomicity is broken.");
  }

  protected abstract RatchetTckRuntime runtime();

  protected abstract RatchetTransactionDriver transactionDriver();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  /**
   * Bound on the negative-assertion wait for the rollback case. A non-trivial wall-clock window so
   * we give a misbehaving store time to fire COMPLETED if it's going to. Keep it short enough that
   * a passing run doesn't drag — the absence of an event is what's being asserted.
   */
  protected Duration rollbackQuietWindow() {
    return Duration.ofMillis(750);
  }
}
