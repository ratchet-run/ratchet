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

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * Base contract: a Jakarta-EE-compliant Ratchet runtime backed by a JTA-aware store MUST honor the
 * caller's transaction context on enqueue. Specifically:
 *
 * <ul>
 *   <li>An enqueue performed inside a committed transaction MUST result in the job being executed
 *       by the scheduler.
 *   <li>An enqueue performed inside a rolled-back transaction MUST NOT result in the job being
 *       executed.
 * </ul>
 *
 * <p>This contract is a conformance grade, not a universal hard requirement. Non-JTA stores are
 * permitted; implementations whose store cannot participate in JTA (e.g., MongoDB without a
 * session) are not "Ratchet Jakarta Runtime Compatible" with respect to transactional enqueue. Such
 * implementations should mark their concrete subclass with {@code @Disabled("Store does not
 * participate in JTA")}.
 *
 * <p>Subclasses provide an Arquillian {@code @Deployment} that bundles this contract package and
 * the implementation's {@link RatchetTckRuntime} adapter.
 */
public abstract class AbstractTxEnqueueContract {

  @Inject protected UserTransaction tx;

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void commitPublishesEnqueuedJob() throws Exception {
    tx.begin();
    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    tx.commit();

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job enqueued in a committed transaction must execute. If this fails on a JTA-aware "
            + "store, the runtime is publishing to its scheduler queue eagerly instead of "
            + "deferring to commit, breaking caller atomicity.");
  }

  @Test
  void rollbackSuppressesEnqueuedJob() throws Exception {
    tx.begin();
    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    tx.rollback();

    assertFalse(
        runtime().probe().awaitCompleted(handle, rollbackQuietWindow()),
        "Job enqueued in a rolled-back transaction must NOT execute. A COMPLETED event for "
            + "handle "
            + handle.id()
            + " indicates the runtime ignored the rollback — caller atomicity is broken.");
  }

  protected abstract RatchetTckRuntime runtime();

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
