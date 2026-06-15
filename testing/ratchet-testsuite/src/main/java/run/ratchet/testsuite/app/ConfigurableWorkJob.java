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

/** Test job with adjustable sleep. */
public class ConfigurableWorkJob {

  private static final AtomicInteger INVOCATION_COUNT = new AtomicInteger(0);
  private static volatile long sleepMs = 5;

  public static void execute() throws InterruptedException {
    Thread.sleep(sleepMs);
    INVOCATION_COUNT.incrementAndGet();
  }

  public static void setSleepMs(long ms) {
    sleepMs = ms;
  }

  public static int getInvocationCount() {
    return INVOCATION_COUNT.get();
  }

  public static void reset() {
    sleepMs = 5;
    INVOCATION_COUNT.set(0);
  }
}
