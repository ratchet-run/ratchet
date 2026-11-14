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
package run.ratchet.testsuite.app;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.TransactionalJob;

public class ResourceTestJob {

  private static final AtomicInteger CONCURRENT = new AtomicInteger();
  private static final AtomicInteger MAX_CONCURRENT = new AtomicInteger();
  private static final AtomicInteger COMPLETED = new AtomicInteger();
  private static final AtomicInteger GATED_RUNNING = new AtomicInteger();
  private static volatile CountDownLatch gate = new CountDownLatch(0);

  public static void execute() throws InterruptedException {
    int current = CONCURRENT.incrementAndGet();
    MAX_CONCURRENT.accumulateAndGet(current, Math::max);
    Thread.sleep(500);
    CONCURRENT.decrementAndGet();
    COMPLETED.incrementAndGet();
  }

  /** Batch-child entry point: each item runs the same permit-gated body as {@link #execute()}. */
  public static void executeItem(String item) {
    try {
      execute();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while processing " + item, e);
    }
  }

  /** Holds its resource permit until {@link #openGate()} is called. */
  public static void executeGated() throws InterruptedException {
    GATED_RUNNING.incrementAndGet();
    try {
      if (!gate.await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Gate was not opened within 60 seconds");
      }
      COMPLETED.incrementAndGet();
    } finally {
      GATED_RUNNING.decrementAndGet();
    }
  }

  /** {@link #executeGated()} inside the job's transaction, completed by the store in it. */
  @TransactionalJob
  public static void executeGatedTransactional() throws InterruptedException {
    executeGated();
  }

  public static void closeGate() {
    gate = new CountDownLatch(1);
  }

  public static void openGate() {
    gate.countDown();
  }

  public static int getGatedRunning() {
    return GATED_RUNNING.get();
  }

  public static int getMaxConcurrentSeen() {
    return MAX_CONCURRENT.get();
  }

  public static int getCompletedCount() {
    return COMPLETED.get();
  }

  public static void reset() {
    CONCURRENT.set(0);
    MAX_CONCURRENT.set(0);
    COMPLETED.set(0);
    openGate();
  }
}
