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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for the happy-path lifecycle of a submitted job: submit → started → completed.
 *
 * <p>Subclasses provide a {@link RatchetTckRuntime} via {@link #runtime()} and may override {@link
 * #defaultTimeout()} for slow runtimes (containers booting JTA, etc.).
 */
public abstract class AbstractJobLifecycleContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void submit_thenStartsAndCompletes() {
    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Submitted no-op job should complete within timeout");

    List<ProbeEvent> events = runtime().probe().events(handle);
    assertTrue(
        events.stream().anyMatch(e -> e.type() == ProbeEvent.Type.STARTED),
        "Lifecycle must include STARTED before COMPLETED. Observed: " + events);
    assertEquals(
        1,
        runtime().probe().invocationCount(handle),
        "Successful no-op job must invoke task body exactly once");
  }

  @Test
  void submit_failingTaskTransitionsToFailed() {
    JobHandle handle =
        runtime().scheduler().enqueue(TckJobs::throwIntentional).withMaxRetries(0).submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitFailed(handle, defaultTimeout()),
        "Failing job with no retries must reach FAILED within timeout");
  }

  /** The runtime under test. Same instance is returned across calls within a single test. */
  protected abstract RatchetTckRuntime runtime();

  /**
   * Default timeout for {@code await*} probe assertions. Override for runtimes that need more
   * generous bounds (e.g. JTA-backed schedulers in a managed container).
   */
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(5);
  }
}
