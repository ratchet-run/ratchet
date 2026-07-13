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
package run.ratchet.ri.core;

import java.time.Duration;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobOptions;

/** Shared mutable builder state for batch-child retry configuration. */
final class BatchChildRetryOptions {

  private JobOptions options = JobOptions.defaults();

  void withBackoff(BackoffPolicy policy, Duration param) {
    options = options.withBackoff(policy, param);
  }

  void withMaxRetries(int retries) {
    options = options.withMaxRetries(retries);
  }

  JobOptions value() {
    return options;
  }
}
