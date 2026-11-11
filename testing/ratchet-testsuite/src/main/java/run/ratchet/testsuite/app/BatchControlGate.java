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

/** Gates batch children while the test controls their parent. */
public final class BatchControlGate {
  private static volatile CountDownLatch gate = new CountDownLatch(1);
  private static final AtomicInteger started = new AtomicInteger();
  private static final AtomicInteger succeeded = new AtomicInteger();
  private static final AtomicInteger failed = new AtomicInteger();

  private BatchControlGate() {}

  public static void reset() {
    gate.countDown();
    gate = new CountDownLatch(1);
    started.set(0);
    succeeded.set(0);
    failed.set(0);
  }

  public static void process(Integer item) {
    started.incrementAndGet();
    try {
      if (!gate.await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("batch control gate timed out");
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("batch control gate interrupted", ex);
    }
  }

  public static void processAndFail(Integer item) {
    process(item);
    throw new IllegalStateException("batch control attempt failed");
  }

  public static void release() {
    gate.countDown();
  }

  public static int started() {
    return started.get();
  }

  public static void success() {
    succeeded.incrementAndGet();
  }

  public static void failure() {
    failed.incrementAndGet();
  }

  public static int successes() {
    return succeeded.get();
  }

  public static int failures() {
    return failed.get();
  }
}
