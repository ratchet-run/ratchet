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
package run.ratchet.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Stored concurrency limit for a named resource.
 *
 * <p>The constructor checks nulls only. Stores can read rows written outside Ratchet. {@link
 * ResourceLimits#setResourceLimit(ResourceLimit)} validates and normalizes values before writing:
 * names are trimmed, nonblank printable ASCII, at most 100 characters; maxConcurrent is at least 1;
 * retryDelay is between zero and 2147483647 milliseconds. Descriptions are trimmed printable ASCII,
 * at most 255 characters, with blank descriptions stored as null.
 *
 * @param resourceName resource name
 * @param maxConcurrent maximum concurrent permits
 * @param retryDelay delay before a job checks again after a permit miss
 * @param description optional description
 */
public record ResourceLimit(
    String resourceName, int maxConcurrent, Duration retryDelay, @Nullable String description) {

  public static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(5);

  public ResourceLimit {
    Objects.requireNonNull(resourceName, "resourceName");
    Objects.requireNonNull(retryDelay, "retryDelay");
  }

  /** Creates a limit with the default retry delay and no description. */
  public static ResourceLimit of(String resourceName, int maxConcurrent) {
    return new ResourceLimit(resourceName, maxConcurrent, DEFAULT_RETRY_DELAY, null);
  }

  /** Returns a copy with the supplied retry delay. */
  public ResourceLimit withRetryDelay(Duration retryDelay) {
    return new ResourceLimit(resourceName, maxConcurrent, retryDelay, description);
  }

  /** Returns a copy with the supplied description. */
  public ResourceLimit withDescription(@Nullable String description) {
    return new ResourceLimit(resourceName, maxConcurrent, retryDelay, description);
  }
}
