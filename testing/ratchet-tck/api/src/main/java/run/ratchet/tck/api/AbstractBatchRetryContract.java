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
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;

/** Base contract for retry configuration on batch child jobs. */
public abstract class AbstractBatchRetryContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void configuredBatchChild_retriesAndLetsTheBatchSucceed() {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueueBatch("retrying-child")
            .forEach(List.of("item"), TckJobs::failFirstBatchChildAttempt)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "A batch must complete after its configured child retry succeeds");
    assertEquals(
        2,
        TckJobs.flakyBatchChildAttempts(),
        "The child must execute once initially and once as a retry");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(15);
  }
}
