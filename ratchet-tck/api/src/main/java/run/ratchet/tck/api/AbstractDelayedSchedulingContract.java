package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for delayed scheduling. Gated on the implementation exposing a {@link TestClock}
 * via {@link RatchetTckRuntime#clock()}; wall-clock-only schedulers (the RI today) skip this whole
 * contract via {@link Assumptions#assumeTrue}.
 */
public abstract class AbstractDelayedSchedulingContract {

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }

  /** Resolved clock from {@link RatchetTckRuntime#clock()} after the assumption check. */
  protected TestClock clock;

  @BeforeEach
  void requireTestClock() {
    Assumptions.assumeTrue(
        runtime().clock().isPresent(),
        "AbstractDelayedSchedulingContract requires RatchetTckRuntime.clock() to be present; "
            + "wall-clock-driven runtimes skip this contract.");
    this.clock = runtime().clock().orElseThrow();
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void scheduledJob_doesNotRunBeforeDelayElapses() {
    Duration delay = Duration.ofMinutes(5);
    Instant submittedAt = clock.now();

    JobHandle handle = runtime().scheduler().schedule(delay, TckJobs::noop).submit();
    runtime().probe().track(handle);

    // Advance halfway through the delay; job must not have executed yet.
    clock.advance(delay.dividedBy(2));
    assertFalse(
        runtime().probe().awaitExecuted(handle, Duration.ofMillis(250)),
        "Scheduled job must not start before its delay has elapsed (clock at "
            + clock.now()
            + ", submitted at "
            + submittedAt
            + ")");

    // Advance past the deadline; now it must run.
    clock.advanceTo(submittedAt.plus(delay).plusSeconds(1));
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Scheduled job must complete once delay has elapsed");
  }
}
