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

/**
 * Simple job for recurring/cron schedule tests.
 *
 * <p>Tracks tick count so tests can verify that the cron schedule fires at the expected rate.
 */
public class CronTestJobs {

  private static final AtomicInteger TICK_COUNT = new AtomicInteger(0);

  public static void tick() {
    TICK_COUNT.incrementAndGet();
  }

  public static int tickCount() {
    return TICK_COUNT.get();
  }

  public static void reset() {
    TICK_COUNT.set(0);
  }
}
