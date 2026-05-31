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
 * TCK contract: builder-factory operations on {@link run.ratchet.api.JobSchedulerService}
 * documented as transaction attribute {@code SUPPORTS} MUST NOT initiate a transaction. Their
 * terminal {@code submit()} call is {@code REQUIRED} and MUST participate in the caller's
 * transaction when one is active, or open its own when none is present.
 *
 * <ul>
 *   <li>{@code submit()} inside a committed caller TX → job executes.
 *   <li>{@code submit()} inside a rolled-back caller TX → job does NOT execute.
 *   <li>{@code submit()} without a caller TX → job executes (submit opens its own TX).
 * </ul>
 *
 * <p>Covers the {@link run.ratchet.api.JobSchedulerService#enqueue} and {@link
 * run.ratchet.api.JobSchedulerService#schedule} builder paths. Batch and streaming-batch builders
 * follow the same rule but are omitted here to keep the contract focused.
 *
 * <p>Implementations backed by a non-JTA store (e.g. MongoDB) should annotate their concrete
 * subclass with {@code @Disabled("Store does not participate in JTA")}.
 */
public abstract class AbstractTxSupportsContract {

  @Inject protected UserTransaction tx;

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void enqueueSubmit_insideCommittedTx_jobExecutes() throws Exception {
    tx.begin();
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);
    tx.commit();

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job submitted via enqueue().submit() inside a committed TX must execute.");
  }

  @Test
  void enqueueSubmit_withoutCallerTx_jobExecutes() {
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Job submitted via enqueue().submit() without a caller TX must execute. submit() must "
            + "open its own transaction when no caller TX is active.");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

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
  protected void enqueueSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    assumeTrue(
        !"mongodb".equals(System.getProperty("ratchet.test.db.type", "")),
        "MongoDB does not participate in JTA rollback");
    tx.begin();
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);
    tx.rollback();

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "Job submitted via enqueue().submit() inside a rolled-back TX must not execute. A "
            + "COMPLETED event here means submit() opened its own TX instead of joining the "
            + "caller's, breaking caller atomicity.");
  }

  /**
   * @apiNote Intentionally {@code protected} so runtime-specific TCK subclasses (e.g. the RI
   *     test-suite under {@code ratchet-testsuite}) can override this test to attach
   *     deployment-specific annotations such as {@code @DisabledIfSystemProperty}. Overriders MUST
   *     delegate to {@code super}; replacing the body silently suppresses the contract.
   */
  @Test
  protected void scheduleSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    assumeTrue(
        !"mongodb".equals(System.getProperty("ratchet.test.db.type", "")),
        "MongoDB does not participate in JTA rollback");
    tx.begin();
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofMillis(100), TckJobs::noop).submit();
    runtime().probe().track(handle);
    tx.rollback();

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "Delayed job submitted via schedule().submit() inside a rolled-back TX must not execute. "
            + "submit() must participate in the caller's TX regardless of the builder path used.");
  }
}
