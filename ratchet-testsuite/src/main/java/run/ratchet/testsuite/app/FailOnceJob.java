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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job that fails on the first execution but succeeds on subsequent attempts. Used to test DLQ
 * manual retry — after the job lands in the DLQ and is retried, it should succeed.
 */
public class FailOnceJob {

  private static final AtomicBoolean HAS_FAILED = new AtomicBoolean(false);
  private static final AtomicInteger ATTEMPT_COUNT = new AtomicInteger(0);

  public static void execute() {
    ATTEMPT_COUNT.incrementAndGet();
    if (HAS_FAILED.compareAndSet(false, true)) {
      throw new RuntimeException("Intentional first-time failure");
    }
    // Subsequent attempts succeed
  }

  public static int getAttemptCount() {
    return ATTEMPT_COUNT.get();
  }

  public static boolean hasSucceeded() {
    return HAS_FAILED.get() && ATTEMPT_COUNT.get() > 1;
  }

  public static void reset() {
    HAS_FAILED.set(false);
    ATTEMPT_COUNT.set(0);
  }
}
