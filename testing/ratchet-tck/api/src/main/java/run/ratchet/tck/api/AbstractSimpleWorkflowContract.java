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

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for chain ordering on {@link run.ratchet.api.JobBuilder#then
 * then(SerializableCheckedRunnable)}.
 *
 * <p>Step bodies are method references on {@link TckJobs} (see {@link TckJobs#recordStepA}, {@link
 * TckJobs#recordStepB}, {@link TckJobs#recordStepC}). Each step appends its label to a process-wide
 * queue so the test can assert observed order across the full chain. {@link TckJobs#resetAll()}
 * clears the queue between tests.
 */
public abstract class AbstractSimpleWorkflowContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void thenChain_executesInDeclaredOrder() throws InterruptedException {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordStepA)
            .then(TckJobs::recordStepB)
            .then(TckJobs::recordStepC)
            .submit();
    runtime().probe().track(handle);

    // The probe handle covers only step-A. Chained tasks (step-B, step-C) execute as separate
    // jobs whose handles aren't returned by the API; wait for the full chain by polling the
    // recorded events until all three are observed or the timeout elapses.
    // 200 ms per poll reduces spin overhead vs the old 50 ms interval. On a loaded CI runner
    // each sleep overshoots to ~100-250 ms anyway; a larger nominal interval avoids burning the
    // 30 s budget 6x faster than intended without changing when the assertion fires.
    long deadlineNanos = System.nanoTime() + defaultTimeout().toNanos();
    while (TckJobs.chainEvents().size() < 3 && System.nanoTime() < deadlineNanos) {
      Thread.sleep(200L);
    }

    assertEquals(
        List.of("step-A", "step-B", "step-C"),
        TckJobs.chainEvents(),
        "Chained tasks must execute in declared order");
  }

  protected abstract RatchetTckRuntime runtime();

  /**
   * Default timeout. Generous because chained tasks execute as separate jobs and each one waits one
   * poll cycle (~2s on the RI) before being picked up; the full chain serializes through the
   * poller. Subclasses may shrink for fast in-memory implementations.
   */
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }
}
