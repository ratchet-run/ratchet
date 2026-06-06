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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobRetryingEvent;

/**
 * Base contract for the documented boolean returns of {@code pauseJob}, {@code resumeJob}, and
 * {@code retryJob}.
 *
 * <p>Only the transaction boundary of these methods had any TCK coverage, and {@code retryJob} had
 * none at all. This contract pins each documented return: pause is idempotent-true on an
 * already-paused job and false on an incompatible state or unknown id; resume is true for a paused
 * job and false otherwise; retry resets a FAILED job to PENDING (returning true, firing {@link
 * JobRetryingEvent}, and letting the body run again) and returns false for any non-FAILED or
 * unknown job.
 */
public abstract class AbstractJobControlReturnContract {

  private final List<Object> events = new CopyOnWriteArrayList<>();
  private Consumer<Object> listener;

  @AfterEach
  void clearAfterEach() {
    if (listener != null) {
      runtime().scheduler().removeEventListener(listener);
      listener = null;
    }
    runtime().clear();
    TckJobs.resetAll();
    events.clear();
  }

  @Test
  void retryJob_failedJob_returnsTrueRerunsBodyAndFiresEvent() {
    listener = events::add;
    runtime().scheduler().addEventListener(listener);

    JobHandle handle =
        runtime().scheduler().enqueue(TckJobs::throwIntentional).withMaxRetries(0).submit();
    runtime().probe().track(handle);
    assertTrue(
        runtime().probe().awaitFailed(handle, defaultTimeout()), "job must reach FAILED first");
    assertEquals(1, runtime().probe().invocationCount(handle), "body runs once before retry");

    assertTrue(
        runtime().scheduler().retryJob(handle.id()), "retryJob on a FAILED job returns true");

    // Reset to PENDING re-enters the poller and runs the body a second time — observable proof of
    // the FAILED -> PENDING transition without peeking at store internals.
    awaitInvocationCount(handle, 2);
    assertTrue(
        events.stream().anyMatch(JobRetryingEvent.class::isInstance),
        "retryJob must publish a JobRetryingEvent");
  }

  @Test
  void retryJob_returnsFalseForUnknownOrNonFailedJob() {
    assertFalse(
        runtime().scheduler().retryJob(UUID.randomUUID()),
        "retryJob on an unknown id returns false");

    JobHandle done = completedJob();
    assertFalse(
        runtime().scheduler().retryJob(done.id()),
        "retryJob on a non-FAILED (SUCCEEDED) job returns false");
  }

  @Test
  void pauseAndResume_returnTrueAndPauseIsIdempotent() {
    JobHandle paused = pauseFreshlyPendingJob();

    assertTrue(
        runtime().scheduler().pauseJob(paused.id()),
        "pausing an already-PAUSED job returns true (idempotent)");
    assertTrue(runtime().scheduler().resumeJob(paused.id()), "resuming a PAUSED job returns true");
    assertTrue(
        runtime().probe().awaitCompleted(paused, defaultTimeout()),
        "a resumed job becomes eligible and runs to completion");
  }

  @Test
  void pauseJob_returnsFalseForUnknownOrIncompatibleState() {
    assertFalse(
        runtime().scheduler().pauseJob(UUID.randomUUID()), "pause on an unknown id returns false");

    JobHandle done = completedJob();
    assertFalse(
        runtime().scheduler().pauseJob(done.id()),
        "pause on a SUCCEEDED job returns false — only PENDING is pausable");
  }

  @Test
  void resumeJob_returnsFalseForUnknownOrNonPausedJob() {
    assertFalse(
        runtime().scheduler().resumeJob(UUID.randomUUID()),
        "resume on an unknown id returns false");

    JobHandle done = completedJob();
    assertFalse(
        runtime().scheduler().resumeJob(done.id()),
        "resume on a non-PAUSED (SUCCEEDED) job returns false");
  }

  /**
   * Submits noop jobs and pauses each as soon as it is created, returning the first one paused
   * before the poller could claim it. The retry loop absorbs the inherent submit/poll race on a
   * live scheduler without a delayed-scheduling primitive; the unpaused stragglers simply run their
   * noop body and complete.
   */
  private JobHandle pauseFreshlyPendingJob() {
    for (int attempt = 0; attempt < 10; attempt++) {
      JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
      runtime().probe().track(handle);
      if (runtime().scheduler().pauseJob(handle.id())) {
        return handle;
      }
    }
    return fail("could not pause a freshly-submitted PENDING job before the poller claimed it");
  }

  private JobHandle completedJob() {
    JobHandle handle = runtime().scheduler().enqueue(TckJobs::noop).submit();
    runtime().probe().track(handle);
    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "setup job must reach SUCCEEDED");
    return handle;
  }

  private void awaitInvocationCount(JobHandle handle, int target) {
    long deadlineNanos = System.nanoTime() + defaultTimeout().toNanos();
    while (runtime().probe().invocationCount(handle) < target) {
      if (System.nanoTime() >= deadlineNanos) {
        fail(
            "expected the body to run "
                + target
                + " times after retry but saw "
                + runtime().probe().invocationCount(handle));
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("interrupted while awaiting re-execution");
      }
    }
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }
}
