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
package run.ratchet.ri.core.internal;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State shared by the worker and watchdog for one execution attempt. Retries get a fresh flag.
 * {@link #requestCancellation()} is the entry point for future sources such as a user-cancel poll.
 * A future fencing token (#223) would also belong here.
 */
public final class JobAttemptControl {
  private final UUID jobId;
  private final Instant deadline;
  private final Instant executionStartTime;
  private final long timeoutSeconds;
  private final AtomicBoolean cancellationRequested = new AtomicBoolean();
  private final AtomicBoolean timeoutClaimed = new AtomicBoolean();

  JobAttemptControl(UUID jobId, Instant deadline, long timeoutSeconds, Instant executionStartTime) {
    this.jobId = Objects.requireNonNull(jobId);
    this.deadline = Objects.requireNonNull(deadline);
    this.timeoutSeconds = timeoutSeconds;
    this.executionStartTime = Objects.requireNonNull(executionStartTime);
  }

  public UUID jobId() {
    return jobId;
  }

  public Instant deadline() {
    return deadline;
  }

  public long timeoutSeconds() {
    return timeoutSeconds;
  }

  public Instant executionStartTime() {
    return executionStartTime;
  }

  public void requestCancellation() {
    cancellationRequested.set(true);
  }

  public boolean isCancellationRequested() {
    return cancellationRequested.get();
  }

  public boolean claimTimeout() {
    return timeoutClaimed.compareAndSet(false, true);
  }
}
