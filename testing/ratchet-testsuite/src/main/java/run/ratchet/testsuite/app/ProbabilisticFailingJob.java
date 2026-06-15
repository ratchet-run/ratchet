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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job with configurable failure probability for testing retry path overhead. Each invocation rolls
 * a random number against the failure rate and throws if below the threshold.
 */
public class ProbabilisticFailingJob {

  private static final AtomicInteger SUCCESS_COUNT = new AtomicInteger(0);
  private static final AtomicInteger FAILURE_COUNT = new AtomicInteger(0);
  private static volatile double failureRate = 0.0;

  public static void execute() {
    if (ThreadLocalRandom.current().nextDouble() < failureRate) {
      FAILURE_COUNT.incrementAndGet();
      throw new RuntimeException("Probabilistic failure (rate=" + failureRate + ")");
    }
    SUCCESS_COUNT.incrementAndGet();
  }

  public static void setFailureRate(double rate) {
    failureRate = rate;
  }

  public static int getSuccessCount() {
    return SUCCESS_COUNT.get();
  }

  public static int getFailureCount() {
    return FAILURE_COUNT.get();
  }

  public static void reset() {
    failureRate = 0.0;
    SUCCESS_COUNT.set(0);
    FAILURE_COUNT.set(0);
  }
}
