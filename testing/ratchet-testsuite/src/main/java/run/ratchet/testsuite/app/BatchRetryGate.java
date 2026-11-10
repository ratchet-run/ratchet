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

/** Static gates shared by the integration test and its deployed job executions. */
public final class BatchRetryGate {
  private static final AtomicInteger attempts = new AtomicInteger();
  private static volatile CountDownLatch release = new CountDownLatch(1);
  private static volatile CountDownLatch entered = new CountDownLatch(1);

  private BatchRetryGate() {}

  public static void reset() {
    release.countDown();
    attempts.set(0);
    release = new CountDownLatch(1);
    entered = new CountDownLatch(1);
  }

  public static void process(String item) {
    if ("A".equals(item)) {
      if (attempts.incrementAndGet() == 1) {
        throw new IllegalStateException("first attempt fails");
      }
    } else {
      entered.countDown();
      try {
        if (!release.await(45, TimeUnit.SECONDS)) {
          throw new IllegalStateException("batch gate timed out");
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("batch gate interrupted", ex);
      }
    }
  }

  public static boolean isBlocked() {
    return entered.getCount() == 0 && release.getCount() != 0;
  }

  public static void release() {
    release.countDown();
  }
}
