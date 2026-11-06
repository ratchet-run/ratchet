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
package run.ratchet.api.internal;

import java.time.Duration;
import java.util.UUID;
import run.ratchet.api.Nullable;

/** In-memory continuation request for the current payload invocation. */
public final class JobContinuation {
  private final UUID jobId;
  private volatile boolean supported = true;
  private @Nullable Duration delay;

  public JobContinuation(UUID jobId) {
    this.jobId = jobId;
  }

  /**
   * Sets the runtime capability before invoking the payload; test contexts default to supported.
   */
  public void supported(boolean supported) {
    this.supported = supported;
  }

  public synchronized void request(Duration delay) {
    if (!supported) {
      throw new UnsupportedOperationException(
          "Job " + jobId + " store does not support job state (JobStateStore)");
    }
    this.delay = delay;
  }

  public synchronized void clear() {
    delay = null;
  }

  public synchronized @Nullable Duration delay() {
    return delay;
  }
}
