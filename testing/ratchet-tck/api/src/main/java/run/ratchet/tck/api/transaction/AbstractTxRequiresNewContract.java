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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * TCK contract: a worker's post-execution terminal transition remains durable independently of a
 * later caller transaction that observes and tentatively replaces the completed job before rolling
 * back.
 */
public abstract class AbstractTxRequiresNewContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Such an override must delegate to {@code super} and must
   *     not replace the assertion body.
   */
  @Test
  protected void completedState_survivesCallerRollback() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");

    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "The setup job must commit its terminal state before the caller transaction begins");

    JobHandle replacementInsideCallerTransaction =
        transactionDriver()
            .rollingBack(
                () -> {
                  assertFalse(
                      runtime().scheduler().cancelJob(handle.id()),
                      "A completed job must already be terminal inside the caller transaction");
                  return runtime()
                      .scheduler()
                      .replace(handle.id(), Duration.ofMinutes(5), TckJobs::noop, null);
                });

    JobHandle replacementAfterRollback =
        transactionDriver()
            .rollingBack(
                () -> {
                  assertFalse(
                      runtime().scheduler().cancelJob(handle.id()),
                      "Rolling back the caller transaction must not erase the worker's committed "
                          + "terminal state");
                  return runtime()
                      .scheduler()
                      .replace(handle.id(), Duration.ofMinutes(5), TckJobs::noop, null);
                });

    assertNotEquals(
        replacementInsideCallerTransaction.id(),
        replacementAfterRollback.id(),
        "The tentative replacement and superseded marker must roll back with the caller "
            + "transaction; a fresh replacement afterward proves the completed job still exists");
  }

  protected abstract RatchetTckRuntime runtime();

  protected abstract RatchetTransactionDriver transactionDriver();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }
}
