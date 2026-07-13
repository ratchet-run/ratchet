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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import run.ratchet.tck.util.ConcurrentTestRunner;

/**
 * Base contract for {@link run.ratchet.api.JobBuilder#withBusinessKey(String) withBusinessKey}
 * semantics.
 *
 * <p>The Ratchet API javadoc on {@code withBusinessKey} states only that the call "prevents
 * concurrent execution against the same entity" and that "multiple completed jobs may share the
 * same key; only active (PENDING/RUNNING) jobs are blocked." It deliberately does NOT lock the
 * rejection <em>mechanism</em>. A conformant implementation may either:
 *
 * <ul>
 *   <li>throw an exception from {@code submit()} (the reference implementation does this), or
 *   <li>return a handle whose {@code id()} points to the existing active job (idempotent merge,
 *       analogous to how {@code withIdempotencyKey} permanently merges).
 * </ul>
 *
 * <p>This contract therefore enforces only the <em>observable</em> property — a duplicate active
 * business key MUST NOT cause a second execution — and tolerates either rejection mechanism.
 *
 * <p>The complementary {@link AbstractIdempotencyContract} pins the stricter {@code
 * withIdempotencyKey} permanent-merge semantics.
 */
public abstract class AbstractBusinessKeyContract {

  private static String uniqueKey(String label) {
    return label + '-' + UUID.randomUUID();
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  /**
   * Sequential variant. Submit a long-running job with key K, await its STARTED event, then submit
   * a second job with the same key. The second submission must either throw or return a handle
   * pointing back to the first job's id — and it must not produce a second invocation.
   */
  @Test
  void duplicateBusinessKeyWhileActive_isRejectedOrMerged() throws InterruptedException {
    String businessKey = uniqueKey("active-dup");
    CountDownLatch firstStarted = TckJobs.beginBlocking();

    JobHandle first =
        runtime()
            .scheduler()
            .enqueue(TckJobs::blockUntilReleased)
            .withBusinessKey(businessKey)
            .submit();
    runtime().probe().track(first);

    assertTrue(
        firstStarted.await(defaultTimeout().toMillis(), TimeUnit.MILLISECONDS),
        "First job must reach RUNNING before duplicate-submit attempt");

    AtomicReference<JobHandle> secondHandle = new AtomicReference<>();
    AtomicReference<Throwable> secondError = new AtomicReference<>();
    try {
      JobHandle second =
          runtime().scheduler().enqueue(TckJobs::noop).withBusinessKey(businessKey).submit();
      runtime().probe().track(second);
      secondHandle.set(second);
    } catch (RuntimeException ex) {
      secondError.set(ex);
    }

    assertTrue(
        secondError.get() != null || secondHandle.get() != null,
        "Duplicate-active submit must either throw OR return a handle (idempotent merge)");

    if (secondHandle.get() != null) {
      assertEquals(
          first.id(),
          secondHandle.get().id(),
          "Idempotent-merge implementations MUST return the existing job's id, not a new one");
    }

    TckJobs.release();
    assertTrue(
        runtime().probe().awaitCompleted(first, defaultTimeout()),
        "First job must complete after release");

    int firstInvocations = runtime().probe().invocationCount(first);
    int secondInvocations =
        secondHandle.get() == null ? 0 : runtime().probe().invocationCount(secondHandle.get());
    assertEquals(
        1,
        firstInvocations + secondInvocations,
        "Exactly one execution must occur across both submissions; observed first="
            + firstInvocations
            + ", second="
            + secondInvocations);
  }

  /**
   * Concurrent variant. Two submitters race to claim the same business key. Exactly one execution
   * must occur.
   */
  @Test
  void concurrentSubmitsWithSameBusinessKey_executeOnce() throws InterruptedException {
    String businessKey = uniqueKey("concurrent-dup");
    CountDownLatch winnerStarted = TckJobs.beginBlocking();
    AtomicReference<JobHandle> handleA = new AtomicReference<>();
    AtomicReference<JobHandle> handleB = new AtomicReference<>();

    JobHandle survivor;
    try {
      List<Throwable> outcomes =
          ConcurrentTestRunner.runAll(
              defaultTimeout().plus(Duration.ofSeconds(2)),
              () -> {
                JobHandle h =
                    runtime()
                        .scheduler()
                        .enqueue(TckJobs::blockUntilReleased)
                        .withBusinessKey(businessKey)
                        .submit();
                handleA.set(h);
                runtime().probe().track(h);
              },
              () -> {
                JobHandle h =
                    runtime()
                        .scheduler()
                        .enqueue(TckJobs::blockUntilReleased)
                        .withBusinessKey(businessKey)
                        .submit();
                handleB.set(h);
                runtime().probe().track(h);
              });

      long submitWinners = outcomes.stream().filter(t -> t == null).count();
      assertTrue(
          submitWinners >= 1,
          "At least one of two concurrent submitters must succeed; outcomes=" + outcomes);

      if (handleA.get() != null && handleB.get() != null) {
        assertEquals(
            handleA.get().id(),
            handleB.get().id(),
            "Two successful submitters must observe the same active job");
      }

      survivor = handleA.get() != null ? handleA.get() : handleB.get();
      assertNotNull(survivor, "Surviving handle must be observable");
      assertTrue(
          winnerStarted.await(defaultTimeout().toMillis(), TimeUnit.MILLISECONDS),
          "Surviving job must start while its business key is still reserved");
    } finally {
      TckJobs.release();
    }

    Duration completionTimeout = defaultTimeout().plus(Duration.ofSeconds(10));
    assertTrue(
        runtime().probe().awaitCompleted(survivor, completionTimeout),
        "Survivor handle must complete within timeout");

    // Sum invocations per DISTINCT job id: a merge implementation hands both submitters the
    // same id, and counting that job twice would fail a conformant merge with 1 + 1 != 1.
    boolean merged =
        handleA.get() != null
            && handleB.get() != null
            && handleA.get().id().equals(handleB.get().id());
    int invocationsOnA =
        handleA.get() == null ? 0 : runtime().probe().invocationCount(handleA.get());
    int invocationsOnB =
        handleB.get() == null || merged ? 0 : runtime().probe().invocationCount(handleB.get());
    assertEquals(
        1,
        invocationsOnA + invocationsOnB,
        "Exactly one execution across concurrent duplicate-business-key submitters; A="
            + invocationsOnA
            + ", B="
            + invocationsOnB
            + ", merged="
            + merged);
  }

  /**
   * After the first job with key K reaches a terminal state, a fresh submission with the same key
   * must succeed and execute independently.
   */
  @Test
  void businessKeyAfterCompletion_canBeReused() {
    String businessKey = uniqueKey("reuse-after-complete");

    JobHandle first =
        runtime().scheduler().enqueue(TckJobs::noop).withBusinessKey(businessKey).submit();
    runtime().probe().track(first);
    assertTrue(
        runtime().probe().awaitCompleted(first, defaultTimeout()),
        "First job must complete before reuse attempt");

    JobHandle second =
        runtime().scheduler().enqueue(TckJobs::noop).withBusinessKey(businessKey).submit();
    runtime().probe().track(second);
    assertTrue(
        runtime().probe().awaitCompleted(second, defaultTimeout()),
        "Second job with reused business key (after completion) must run to completion");

    assertEquals(1, runtime().probe().invocationCount(first), "First job invoked exactly once");
    assertEquals(1, runtime().probe().invocationCount(second), "Second job invoked exactly once");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }
}
