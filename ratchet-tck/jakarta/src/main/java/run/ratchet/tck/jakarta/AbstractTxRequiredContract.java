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
 * <p>Implementations backed by a non-JTA store (e.g. MongoDB) should annotate their concrete
 * subclass with {@code @Disabled("Store does not participate in JTA")}.
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

  @Test
  void cancelJob_rollback_isNotVisible() throws Exception {
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

  @Test
  protected void pauseJob_rollback_doesNotSuppressExecution() throws Exception {
    assumeTrue(
        !"mongodb".equals(System.getProperty("ratchet.test.db.type", "")),
        "MongoDB does not participate in JTA rollback");
    // 500 ms delay: short enough that the job executes during the test window; long enough to
    // issue begin → pause → rollback before the scheduler can claim it.
    JobHandle handle =
        runtime().scheduler().schedule(Duration.ofMillis(500), TckJobs::noop).submit();
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
