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

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.Recurring;

/**
 * CDI bean with {@code @Recurring} annotated methods for testing recurring job discovery and
 * scheduling.
 */
@ApplicationScoped
public class TestRecurringJobs {

  public static final String EVERY_FIVE_SECONDS_JOB_ID = "test-every-5-seconds";

  private static final AtomicInteger EVERY_FIVE_SECONDS_COUNT = new AtomicInteger(0);

  public static int getEveryFiveSecondsCount() {
    return EVERY_FIVE_SECONDS_COUNT.get();
  }

  public static void resetCounts() {
    EVERY_FIVE_SECONDS_COUNT.set(0);
  }

  @Recurring(cron = "*/5 * * * * ?", id = EVERY_FIVE_SECONDS_JOB_ID)
  public void everyFiveSeconds() {
    EVERY_FIVE_SECONDS_COUNT.incrementAndGet();
  }
}
