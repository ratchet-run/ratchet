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
package run.ratchet.quarkus.it.tck;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Duration;
import run.ratchet.spi.RetryPolicy;

/**
 * A {@link RetryPolicy} that permits the first retry then vetoes. A deterministically failing job
 * runs its body exactly twice, regardless of a larger {@code maxRetries}.
 */
@Alternative
@ApplicationScoped
public class SecondAttemptVetoRetryPolicy implements RetryPolicy {

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    return attempt < 2;
  }

  @Override
  public Duration getDelay(int attempt) {
    return Duration.ZERO;
  }
}
