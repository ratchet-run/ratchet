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
package run.ratchet.tck.api.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.ProbeEvent;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * TCK contract: when a job exhausts its retry policy, Ratchet MUST publish the terminal failed
 * event before the dead-letter event for that same job.
 */
public abstract class AbstractTxDlqOrderContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void exhaustedRetries_publishFailedBeforeDlq() throws InterruptedException {
    int maxRetries = 2;
    JobHandle handle =
        transactionDriver()
            .committing(
                () -> {
                  JobHandle submitted =
                      runtime()
                          .scheduler()
                          .enqueue(TckJobs::throwIntentional)
                          .withMaxRetries(maxRetries)
                          .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
                          .submit();
                  runtime().probe().track(submitted);
                  return submitted;
                });

    assertTrue(
        awaitEventType(handle, ProbeEvent.Type.DLQ, defaultTimeout()),
        "A job that exhausts retries must publish a DLQ event");

    List<ProbeEvent> events = runtime().probe().events(handle);
    int failedIndex = indexOf(events, ProbeEvent.Type.FAILED);
    int dlqIndex = indexOf(events, ProbeEvent.Type.DLQ);

    assertTrue(failedIndex >= 0, "Retry exhaustion must publish a FAILED event: " + events);
    assertTrue(dlqIndex >= 0, "Retry exhaustion must publish a DLQ event: " + events);
    assertTrue(
        failedIndex < dlqIndex,
        "The FAILED event must be observed strictly before the DLQ event: " + events);
    assertEquals(
        maxRetries + 1,
        runtime().probe().invocationCount(handle),
        "The job must exhaust every configured retry before entering the DLQ");
  }

  protected abstract RatchetTckRuntime runtime();

  protected abstract RatchetTransactionDriver transactionDriver();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }

  private boolean awaitEventType(JobHandle handle, ProbeEvent.Type type, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    do {
      if (indexOf(runtime().probe().events(handle), type) >= 0) {
        return true;
      }
      Thread.sleep(10);
    } while (System.nanoTime() < deadline);
    return false;
  }

  private static int indexOf(List<ProbeEvent> events, ProbeEvent.Type type) {
    for (int i = 0; i < events.size(); i++) {
      if (events.get(i).type() == type) {
        return i;
      }
    }
    return -1;
  }
}
