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
package run.ratchet.tck.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Runs tasks in parallel on a shared start latch and returns one Throwable slot per task. */
public final class ConcurrentTestRunner {

  private ConcurrentTestRunner() {}

  /**
   * @return list of per-task results (null on success, otherwise the thrown Throwable)
   */
  public static List<Throwable> runAll(Duration timeout, Runnable... tasks) {
    if (tasks == null || tasks.length == 0) {
      throw new IllegalArgumentException("tasks must be non-null and non-empty");
    }
    Throwable[] results = new Throwable[tasks.length];
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(tasks.length);
    ExecutorService executor = Executors.newFixedThreadPool(tasks.length);
    try {
      for (int i = 0; i < tasks.length; i++) {
        final int slot = i;
        final Runnable task = tasks[i];
        executor.submit(
            () -> {
              try {
                startLatch.await();
                task.run();
              } catch (Throwable t) {
                results[slot] = t;
              } finally {
                doneLatch.countDown();
              }
            });
      }
      startLatch.countDown();
      boolean completed;
      try {
        completed = doneLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("ConcurrentTestRunner interrupted while awaiting task completion");
        return List.of();
      }
      if (!completed) {
        fail(
            "ConcurrentTestRunner timed out after "
                + timeout
                + " waiting for "
                + tasks.length
                + " task(s) to complete");
      }
      return Arrays.asList(results);
    } finally {
      executor.shutdownNow();
    }
  }
}
