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
package run.ratchet.ri.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import run.ratchet.spi.RetryPolicy;

/**
 * Default {@link RetryPolicy} that defers all retry decisions to the job's configured max-retries
 * and backoff policy. Always returns {@code true}/{@link Duration#ZERO}; override with an
 * {@code @Alternative @Priority(APPLICATION) RetryPolicy} bean for custom logic.
 *
 * <p>This policy is a pass-through. Callers must enforce the job's max-attempt bound before calling
 * {@link #shouldRetry(int, Throwable)}.
 */
@ApplicationScoped
public class DefaultRetryPolicy implements RetryPolicy {

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    return true;
  }

  @Override
  public Duration getDelay(int attempt) {
    return Duration.ZERO;
  }
}
