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
package run.ratchet.quarkus.it.tck;

import jakarta.inject.Singleton;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.store.dto.JobCompletionPlan;

/** Test-controlled failures; inactive for every other integration test. */
@Singleton
public class CompletionFaults {
  public final AtomicBoolean corruptNextChainCompletion = new AtomicBoolean();
  public final AtomicBoolean pauseNextRetry = new AtomicBoolean();
  public final AtomicBoolean rejectBatchParents = new AtomicBoolean();
  public final AtomicInteger rejectedParents = new AtomicInteger();
  public volatile JobCompletionPlan failedPlan;
  public volatile CountDownLatch rolledBack = new CountDownLatch(1);
  public volatile CountDownLatch resume = new CountDownLatch(1);

  public void reset() {
    resume.countDown();
    corruptNextChainCompletion.set(false);
    pauseNextRetry.set(false);
    rejectBatchParents.set(false);
    rejectedParents.set(0);
    failedPlan = null;
    rolledBack = new CountDownLatch(1);
    resume = new CountDownLatch(1);
  }
}
