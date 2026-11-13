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
  private volatile boolean transactional;
  private @Nullable Duration delay;

  /**
   * Creates a continuation request for the job, initially with no delay requested.
   *
   * @param jobId job identifier used in unsupported-store errors
   */
  public JobContinuation(UUID jobId) {
    this.jobId = jobId;
  }

  /**
   * Sets the runtime capability before invoking the payload; test contexts default to supported.
   */
  public void supported(boolean supported) {
    this.supported = supported;
  }

  /** Marks a transactional job, whose body cannot request continuation. */
  public void transactional(boolean transactional) {
    this.transactional = transactional;
  }

  /**
   * Records the delay, replacing any previous continuation request.
   *
   * @param delay delay before the next execution
   * @throws IllegalStateException if the body is a transactional job
   * @throws UnsupportedOperationException if the runtime store does not support job state
   */
  public synchronized void request(Duration delay) {
    if (transactional) {
      throw new IllegalStateException("@TransactionalJob does not support requestContinuation()");
    }
    if (!supported) {
      throw new UnsupportedOperationException(
          "Job " + jobId + " store does not support job state (JobStateStore)");
    }
    this.delay = delay;
  }

  /**
   * Clears the request. Ratchet calls this before each payload invocation it makes, including calls
   * repeated by a resilience strategy. An interceptor that re-invokes the payload internally does
   * not call this method.
   */
  public synchronized void clear() {
    delay = null;
  }

  /**
   * Returns the currently requested delay.
   *
   * @return the delay, or null when no continuation is requested
   */
  public synchronized @Nullable Duration delay() {
    return delay;
  }
}
