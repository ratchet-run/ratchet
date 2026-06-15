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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.tck.util.ConcurrentTestRunner;

/**
 * Base contract for {@link run.ratchet.api.JobBuilder#withIdempotencyKey(String)
 * withIdempotencyKey} semantics.
 *
 * <p>Unlike a business key, an idempotency key maps to exactly one job permanently: "once consumed
 * this key is never reusable." This contract mandates the stronger of the two observable behaviors
 * a permanent dedup could take — <b>merge, not reject</b>. A duplicate idempotency key MUST return
 * the <em>original</em> job's {@link JobHandle} so the caller gets a usable handle back, MUST NOT
 * start a second execution, and MUST hold even after the original job has reached a terminal state
 * and even when the duplicate carries a different task or config. Under a <em>concurrent</em> race
 * on the same key the losing submit MAY surface an error instead of merging, but the key must still
 * bind to exactly one job, execute exactly once, and merge on any subsequent re-submit — see {@link
 * #concurrentDuplicateIdempotencyKey_convergesOnSingleJob()}. The complementary {@link
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

  /**
   * Concurrent variant. Two callers race the same idempotency key. The merge mandate applies to the
   * <em>observable-duplicate</em> case; under a true race the loser MAY surface an error (a
   * unique-constraint loss aborts the surrounding transaction on some SQL stores, so merge cannot
   * be required portably). What MUST hold regardless of which caller wins:
   *
   * <ul>
   *   <li>at least one submission succeeds,
   *   <li>every successful submission returns the <em>same</em> job id (never two jobs),
   *   <li>the task executes exactly once, and
   *   <li>a subsequent re-submit with the same key converges on the original handle — the key is
   *       permanently bound to the single winning job.
   * </ul>
   */
  @Test
  void concurrentDuplicateIdempotencyKey_convergesOnSingleJob() {
    String key = freshKey();
    AtomicReference<JobHandle> handleA = new AtomicReference<>();
    AtomicReference<JobHandle> handleB = new AtomicReference<>();

    List<Throwable> outcomes =
        ConcurrentTestRunner.runAll(
            defaultTimeout().plus(Duration.ofSeconds(2)),
            () -> {
              JobHandle h =
                  runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(key).submit();
              handleA.set(h);
              runtime().probe().track(h);
            },
            () -> {
              JobHandle h =
                  runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(key).submit();
              handleB.set(h);
              runtime().probe().track(h);
            });

    long submitWinners = outcomes.stream().filter(t -> t == null).count();
    assertTrue(
        submitWinners >= 1,
        "At least one of two concurrent same-key submitters must succeed; outcomes=" + outcomes);
    if (handleA.get() != null && handleB.get() != null) {
      assertEquals(
          handleA.get().id(),
          handleB.get().id(),
          "Concurrent same-key submits that both succeed MUST agree on a single job id");
    }

    JobHandle winner = handleA.get() != null ? handleA.get() : handleB.get();
    assertTrue(
        runtime().probe().awaitCompleted(winner, defaultTimeout().plus(Duration.ofSeconds(10))),
        "The single winning job must complete");
    assertEquals(
        1,
        runtime().probe().invocationCount(winner),
        "Exactly one execution may occur across concurrent same-key submitters");

    // Permanence under race: whichever caller lost (or errored), the key is now bound to the
    // winning job, so a clean re-submit MUST merge onto the original handle.
    JobHandle retry = runtime().scheduler().enqueue(TckJobs::noop).withIdempotencyKey(key).submit();
    assertEquals(
        winner.id(),
        retry.id(),
        "A re-submit after the race must return the original job's handle (merge), not a new id");
    assertEquals(
        1,
        runtime().probe().invocationCount(winner),
        "The re-submit must not trigger a second execution");
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

  /**
   * Negative-assertion window used to assert a duplicate does NOT execute. Must comfortably exceed
   * a full poller backoff cycle of the implementation under test — a window shorter than one poll
   * interval passes vacuously because the erroneous execution has not had a chance to start yet.
   * (The RI test deployment backs off to 2 s between polls when idle.)
   */
  protected Duration quietWindow() {
    return Duration.ofSeconds(3);
  }
}
