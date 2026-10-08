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
import java.util.concurrent.atomic.AtomicReference;

/**
 * State shared by the worker and watchdog for one execution attempt. Retries get a fresh flag.
 * {@link #requestCancellation()} is the entry point for future sources such as a user-cancel poll.
 * Its attempt token identifies the attempt in memory; a future fencing token (#223) could build on
 * it.
 */
public final class JobAttemptControl {
  private final Object attemptToken = new Object();
  private final UUID jobId;
  private final Instant deadline;
  private final Instant executionStartTime;
  private final long timeoutSeconds;
  private final int baselineAttempts;
  private final AtomicBoolean cancellationRequested = new AtomicBoolean();

  private enum TimeoutOwner {
    UNCLAIMED,
    WORKER,
    WATCHDOG,
    HANDED_BACK,
    WORKER_WATCHDOG_PASSED
  }

  private final AtomicReference<TimeoutOwner> timeoutOwner =
      new AtomicReference<>(TimeoutOwner.UNCLAIMED);

  JobAttemptControl(
      UUID jobId,
      Instant deadline,
      long timeoutSeconds,
      Instant executionStartTime,
      int baselineAttempts) {
    this.jobId = Objects.requireNonNull(jobId);
    this.deadline = Objects.requireNonNull(deadline);
    this.timeoutSeconds = timeoutSeconds;
    this.baselineAttempts = baselineAttempts;
    this.executionStartTime = Objects.requireNonNull(executionStartTime);
  }

  /**
   * Opaque identity of this attempt, recorded by CancellationRequestedException so an exception
   * kept from an earlier attempt is not taken as this attempt's cooperative stop.
   */
  public Object attemptToken() {
    return attemptToken;
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

  /** Attempt count persisted when this execution attempt started. */
  public int baselineAttempts() {
    return baselineAttempts;
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

  /** Claims the timeout transition for the cooperative worker. */
  public boolean claimTimeoutForWorker() {
    return timeoutOwner.compareAndSet(TimeoutOwner.UNCLAIMED, TimeoutOwner.WORKER);
  }

  /** Claims the timeout for the watchdog, or records that it passed a worker-owned timeout. */
  public boolean claimTimeoutForWatchdog() {
    while (true) {
      TimeoutOwner owner = timeoutOwner.get();
      if (owner == TimeoutOwner.UNCLAIMED || owner == TimeoutOwner.HANDED_BACK) {
        if (timeoutOwner.compareAndSet(owner, TimeoutOwner.WATCHDOG)) {
          return true;
        }
      } else if (owner == TimeoutOwner.WORKER) {
        if (timeoutOwner.compareAndSet(owner, TimeoutOwner.WORKER_WATCHDOG_PASSED)) {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Hands the timeout back, returning false if the watchdog already passed or ownership changed.
   */
  public boolean handBackTimeoutToWatchdog() {
    while (true) {
      TimeoutOwner owner = timeoutOwner.get();
      if (owner != TimeoutOwner.WORKER) {
        return false;
      }
      if (timeoutOwner.compareAndSet(owner, TimeoutOwner.HANDED_BACK)) {
        return true;
      }
    }
  }

  /** Reports whether a failed cooperative transition is waiting for the watchdog. */
  public boolean isTimeoutHandedBack() {
    return timeoutOwner.get() == TimeoutOwner.HANDED_BACK;
  }
}
