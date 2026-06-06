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

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for the broadcast {@code deliverSignal(signalKey, payload)} overload.
 *
 * <p>A broadcast atomically moves every WAITING job sharing the {@code signalKey} to PENDING,
 * returns the number of jobs it transitioned, and leaves waiters on any other key untouched. The
 * single-job overload is exercised by {@link AbstractSignalDecisionContract}; this contract pins
 * the fan-out semantics of the keyed overload.
 */
public abstract class AbstractBroadcastSignalContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void broadcastUnblocksEveryMatchingWaiterAndCountsThem() {
    String key = "broadcast-tck-match";
    JobHandle a = waitingJob(key);
    JobHandle b = waitingJob(key);
    JobHandle c = waitingJob(key);
    JobHandle other = waitingJob("broadcast-tck-other");

    int unblocked = runtime().scheduler().deliverSignal(key, "go");

    assertEquals(
        3, unblocked, "broadcast must return the count of WAITING jobs it moved to PENDING");
    assertTrue(runtime().probe().awaitCompleted(a, defaultTimeout()), "matching waiter a must run");
    assertTrue(runtime().probe().awaitCompleted(b, defaultTimeout()), "matching waiter b must run");
    assertTrue(runtime().probe().awaitCompleted(c, defaultTimeout()), "matching waiter c must run");
    assertFalse(
        runtime().probe().awaitExecuted(other, quietWindow()),
        "a waiter on a different signalKey must stay WAITING after the broadcast");
  }

  @Test
  void broadcastWithNoMatchingWaiterReturnsZero() {
    waitingJob("broadcast-tck-present");

    assertEquals(
        0,
        runtime().scheduler().deliverSignal("broadcast-tck-absent", "go"),
        "broadcasting a key that no job waits on must return 0");
  }

  private JobHandle waitingJob(String signalKey) {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::noop)
            .awaitSignal(signalKey, signalWaitTimeout())
            .submit();
    runtime().probe().track(handle);
    return handle;
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  /** Generous so a waiter never times out before the broadcast lands. */
  protected Duration signalWaitTimeout() {
    return Duration.ofMinutes(1);
  }

  protected Duration quietWindow() {
    return Duration.ofMillis(1000);
  }
}
