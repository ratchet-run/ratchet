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
 * TCK contract: builder-factory operations on {@link JobSchedulerService} documented as transaction
 * attribute {@code SUPPORTS} MUST NOT initiate a transaction. Their terminal {@code submit()} call
 * is {@code REQUIRED} and MUST participate in the caller's transaction when one is active, or open
 * its own when none is present.
 *
 * <ul>
 *   <li>{@code submit()} inside a committed caller TX → job executes.
 *   <li>{@code submit()} inside a rolled-back caller TX → job does NOT execute.
 *   <li>{@code submit()} without a caller TX → job executes (submit opens its own TX).
 * </ul>
 *
 * <p>Covers the {@link JobSchedulerService#enqueue} and {@link JobSchedulerService#schedule}
 * builder paths. Batch and streaming-batch builders follow the same rule but are omitted here to
 * keep the contract focused.
 *
 * <p>Implementations backed by a non-transactional store return {@code false} from {@link
 * RatchetTckRuntime#supportsCallerTransactionRollback()}. Only the rollback cases report {@code
 * N/A}; commit-visible cases still run and must pass.
 */
public abstract class AbstractTxSupportsContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void enqueueSubmit_insideCommittedTx_jobExecutes() throws Exception {
    JobHandle handle =
        transactionDriver()
            .committing(
                () -> {
                  JobHandle submitted = runtime().scheduler().enqueue(TckJobs::noop).submit();
                  runtime().probe().track(submitted);
                  return submitted;
                });

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

  protected abstract RatchetTransactionDriver transactionDriver();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  protected Duration quietWindow() {
    return Duration.ofMillis(750);
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Such an override must delegate to {@code super} and must
   *     not replace the assertion body.
   */
  @Test
  protected void enqueueSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");
    JobHandle handle =
        transactionDriver()
            .rollingBack(
                () -> {
                  JobHandle submitted = runtime().scheduler().enqueue(TckJobs::noop).submit();
                  runtime().probe().track(submitted);
                  return submitted;
                });

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "Job submitted via enqueue().submit() inside a rolled-back TX must not execute. A "
            + "COMPLETED event here means submit() opened its own TX instead of joining the "
            + "caller's, breaking caller atomicity.");
  }

  /**
   * @apiNote Intentionally {@code protected} so a runtime-specific subclass may attach a
   *     runner-level skip annotation when its test harness cannot translate an in-container
   *     assumption into a skipped test. Such an override must delegate to {@code super} and must
   *     not replace the assertion body.
   */
  @Test
  protected void scheduleSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    assumeTrue(
        runtime().supportsCallerTransactionRollback(),
        "The runtime reports that its store does not participate in caller transaction rollback");
    JobHandle handle =
        transactionDriver()
            .rollingBack(
                () -> {
                  JobHandle submitted =
                      runtime()
                          .scheduler()
                          .schedule(Duration.ofMillis(100), TckJobs::noop)
                          .submit();
                  runtime().probe().track(submitted);
                  return submitted;
                });

    assertFalse(
        runtime().probe().awaitCompleted(handle, quietWindow()),
        "Delayed job submitted via schedule().submit() inside a rolled-back TX must not execute. "
            + "submit() must participate in the caller's TX regardless of the builder path used.");
  }
}
