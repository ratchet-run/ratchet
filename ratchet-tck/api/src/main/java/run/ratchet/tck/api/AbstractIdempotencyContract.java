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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;

/**
 * Base contract for {@link run.ratchet.api.JobBuilder#withIdempotencyKey(String)
 * withIdempotencyKey} semantics.
 *
 * <p>Unlike a business key, an idempotency key maps to exactly one job permanently: "once consumed
 * this key is never reusable." This contract mandates the stronger of the two observable behaviors
 * a permanent dedup could take — <b>merge, not reject</b>. A duplicate idempotency key MUST return
 * the <em>original</em> job's {@link JobHandle} so the caller gets a usable handle back, MUST NOT
 * start a second execution, and MUST hold even after the original job has reached a terminal state
 * and even when the duplicate carries a different task or config. The complementary {@link
 * AbstractBusinessKeyContract} pins the looser active-only {@code withBusinessKey} semantics and
 * tolerates either a throw or a merge.
 */
public abstract class AbstractIdempotencyContract {

  // Idempotency keys are capped at 36 characters (a UUID), so a bare UUID is the longest unique key
  // available — a labelled prefix would overflow the limit.
  private static String freshKey() {
    return UUID.randomUUID().toString();
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  /**
   * A duplicate idempotency key submitted while the first job is still active MUST return the
   * original job's handle and MUST NOT cause a second execution.
   */
  @Test
  void duplicateIdempotencyKeyWhileActive_returnsOriginalHandle() throws InterruptedException {
    String key = freshKey();
    CountDownLatch firstStarted = TckJobs.beginBlocking();

    JobHandle first =
        runtime().scheduler().enqueue(TckJobs::blockUntilReleased).withIdempotencyKey(key).submit();
    runtime().probe().track(first);
    assertTrue(
        firstStarted.await(defaultTimeout().toMillis(), TimeUnit.MILLISECONDS),
        "First job must reach RUNNING before the duplicate submit");

    JobHandle second =
        runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(key).submit();
    assertEquals(
        first.id(),
        second.id(),
        "A duplicate idempotency key MUST return the original job's handle (merge), not a new id");

    TckJobs.release();
    assertTrue(
        runtime().probe().awaitCompleted(first, defaultTimeout()),
        "The original job must complete after release");
    assertEquals(1, runtime().probe().invocationCount(first), "Only the original job may execute");
  }

  /**
   * The idempotency key is reserved permanently. After the first job reaches a terminal state, a
   * re-submit with the same key MUST return the original handle, and a changed task or config on
   * the duplicate MUST be ignored rather than executed.
   */
  @Test
  void idempotencyKeyReservedPermanentlyAfterCompletion() {
    String key = freshKey();

    JobHandle first = runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(key).submit();
    runtime().probe().track(first);
    assertTrue(
        runtime().probe().awaitCompleted(first, defaultTimeout()),
        "First job must complete before the re-submit");

    // Re-submit with the same key but a different task and priority. The key is permanently
    // reserved, so this must return the original (completed) job and the changed task must not run.
    JobHandle second =
        runtime()
            .scheduler()
            .enqueue(TckJobs::throwIntentional)
            .withIdempotencyKey(key)
            .withPriority(JobPriority.CRITICAL)
            .submit();
    runtime().probe().track(second);

    assertEquals(
        first.id(),
        second.id(),
        "A completed job permanently reserves its key; re-submit must return the original handle");
    assertFalse(
        runtime().probe().awaitFailed(second, quietWindow()),
        "The changed duplicate task (throwIntentional) must not execute");
    assertEquals(
        1, runtime().probe().invocationCount(first), "The original job executed exactly once");
  }

  /** Distinct idempotency keys are independent: both jobs run. */
  @Test
  void distinctIdempotencyKeys_executeIndependently() {
    JobHandle a =
        runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(freshKey()).submit();
    JobHandle b =
        runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(freshKey()).submit();
    runtime().probe().track(a);
    runtime().probe().track(b);

    assertNotEquals(a.id(), b.id(), "Distinct idempotency keys must produce distinct jobs");
    assertTrue(runtime().probe().awaitCompleted(a, defaultTimeout()), "Job a must complete");
    assertTrue(runtime().probe().awaitCompleted(b, defaultTimeout()), "Job b must complete");
    assertEquals(1, runtime().probe().invocationCount(a), "Job a invoked exactly once");
    assertEquals(1, runtime().probe().invocationCount(b), "Job b invoked exactly once");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }

  /** Negative-assertion window: long enough to catch an erroneous extra execution, short to run. */
  protected Duration quietWindow() {
    return Duration.ofMillis(750);
  }
}
