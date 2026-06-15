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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for delayed scheduling. Gated on the implementation exposing a {@link TestClock}
 * via {@link RatchetTckRuntime#clock()}; wall-clock-only schedulers (the RI today) skip this whole
 * contract via {@link Assumptions#assumeTrue}.
 */
public abstract class AbstractDelayedSchedulingContract {

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

  @Test
  void cancelledBeforeDelayElapses_neverRuns() {
    Duration delay = Duration.ofMinutes(5);
    Instant submittedAt = clock.now();

    JobHandle handle = runtime().scheduler().schedule(delay, TckJobs::noop).submit();
    runtime().probe().track(handle);

    // Cancel while still PENDING (clock has not advanced past the schedule).
    assertTrue(
        runtime().scheduler().cancelJob(handle.id()),
        "cancelJob must succeed for a PENDING delayed job");

    // Advance past the deadline; cancelled job must not execute.
    clock.advanceTo(submittedAt.plus(delay).plusSeconds(1));
    assertFalse(
        runtime().probe().awaitExecuted(handle, Duration.ofMillis(250)),
        "Cancelled job must not start even after its delay has elapsed");
    assertEquals(
        0, runtime().probe().invocationCount(handle), "Cancelled job must have zero invocations");
  }

  @Test
  void clockRewind_isRejected() {
    Duration delay = Duration.ofMinutes(5);
    Instant submittedAt = clock.now();

    JobHandle handle = runtime().scheduler().schedule(delay, TckJobs::noop).submit();
    runtime().probe().track(handle);

    // Advance past the deadline so the job completes.
    clock.advanceTo(submittedAt.plus(delay).plusSeconds(1));
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Scheduled job must complete once delay has elapsed");

    // Attempting to rewind the clock must throw — the contract requires monotonic time.
    Instant past = submittedAt;
    assertThrows(
        IllegalArgumentException.class,
        () -> clock.advanceTo(past),
        "TestClock.advanceTo(target) must reject targets earlier than now()");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }
}
