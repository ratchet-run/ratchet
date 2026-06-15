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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.spi.RetryPolicy;

/**
 * A {@link RetryPolicy} that always vetoes retries, used by tests that need to assert a job fails
 * without retrying.
 *
 * <p><strong>Test lifecycle:</strong> {@link #resetCounts()} must be called in an
 * {@code @BeforeEach} method to prevent retry counts from one test bleeding into the next.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class VetoRetryPolicy implements RetryPolicy {

  private static final AtomicInteger SHOULD_RETRY_COUNT = new AtomicInteger(0);

  public static int getShouldRetryCount() {
    return SHOULD_RETRY_COUNT.get();
  }

  public static void resetCounts() {
    SHOULD_RETRY_COUNT.set(0);
  }

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    SHOULD_RETRY_COUNT.incrementAndGet();
    return false;
  }

  @Override
  public Duration getDelay(int attempt) {
    return Duration.ZERO;
  }
}
