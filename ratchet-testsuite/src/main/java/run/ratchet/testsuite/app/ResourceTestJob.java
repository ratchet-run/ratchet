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

import java.util.concurrent.atomic.AtomicInteger;

public class ResourceTestJob {

  private static final AtomicInteger CONCURRENT = new AtomicInteger();
  private static final AtomicInteger MAX_CONCURRENT = new AtomicInteger();
  private static final AtomicInteger COMPLETED = new AtomicInteger();

  public static void execute() throws InterruptedException {
    int current = CONCURRENT.incrementAndGet();
    MAX_CONCURRENT.accumulateAndGet(current, Math::max);
    Thread.sleep(500);
    CONCURRENT.decrementAndGet();
    COMPLETED.incrementAndGet();
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
  }
}
