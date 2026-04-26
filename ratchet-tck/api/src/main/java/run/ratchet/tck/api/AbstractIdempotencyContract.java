package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for {@link run.ratchet.api.JobBuilder#withBusinessKey(String)
 * withBusinessKey} semantics.
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
 */
public abstract class AbstractIdempotencyContract {

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
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
  void concurrentSubmitsWithSameBusinessKey_executeOnce() {
    String businessKey = uniqueKey("concurrent-dup");
    AtomicReference<JobHandle> handleA = new AtomicReference<>();
    AtomicReference<JobHandle> handleB = new AtomicReference<>();

    List<Throwable> outcomes =
        ConcurrentTestRunner.runAll(
            defaultTimeout().plus(Duration.ofSeconds(2)),
            () -> {
              JobHandle h =
                  runtime()
                      .scheduler()
                      .enqueue(TckJobs::noop)
                      .withBusinessKey(businessKey)
                      .submit();
              handleA.set(h);
              runtime().probe().track(h);
            },
            () -> {
              JobHandle h =
                  runtime()
                      .scheduler()
                      .enqueue(TckJobs::noop)
                      .withBusinessKey(businessKey)
                      .submit();
              handleB.set(h);
              runtime().probe().track(h);
            });

    long submitWinners = outcomes.stream().filter(t -> t == null).count();
    assertTrue(
        submitWinners >= 1,
        "At least one of two concurrent submitters must succeed; outcomes=" + outcomes);

    JobHandle survivor = handleA.get() != null ? handleA.get() : handleB.get();
    assertNotNull(survivor, "Surviving handle must be observable");
    assertTrue(
        runtime().probe().awaitCompleted(survivor, defaultTimeout()),
        "Survivor handle must complete within timeout");

    int invocationsOnA =
        handleA.get() == null ? 0 : runtime().probe().invocationCount(handleA.get());
    int invocationsOnB =
        handleB.get() == null ? 0 : runtime().probe().invocationCount(handleB.get());
    assertEquals(
        1,
        invocationsOnA + invocationsOnB,
        "Exactly one execution across concurrent duplicate-business-key submitters; A="
            + invocationsOnA
            + ", B="
            + invocationsOnB);
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

  private static String uniqueKey(String label) {
    return label + '-' + UUID.randomUUID();
  }
}
