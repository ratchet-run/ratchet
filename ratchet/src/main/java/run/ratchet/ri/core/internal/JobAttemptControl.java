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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;
import run.ratchet.api.AttemptToken;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.JobClaimStore;

/**
 * State shared by the worker and watchdog for one execution attempt. Retries get a fresh flag. A
 * user cancel or replacement can also request a stop. Requests never interrupt the worker. Its
 * cancellation identity identifies the attempt in memory; its claim sequence fences persisted owner
 * writes.
 */
public final class JobAttemptControl {
  private static final Logger log = Logger.getLogger(JobAttemptControl.class);
  private volatile @Nullable Runnable stateFlush;

  public void attachStateFlush(Runnable hook) {
    stateFlush = hook;
  }

  public void flushStateQuietly() {
    Runnable hook = stateFlush;
    if (hook == null) return;
    try {
      hook.run();
    } catch (Throwable failure) {
      log.warnf(failure, "State flush failed for job %s", jobId);
    }
  }

  private final Object cancellationIdentity = new Object();
  private final UUID jobId;
  private final Instant deadline;
  private final Instant executionStartTime;
  private final long timeoutSeconds;
  private final int baselineAttempts;

  /**
   * The claim this attempt runs under; watchdog and cooperative stops fence their writes with it.
   */
  private final long claimSeq;

  private final @Nullable String nodeId;
  private final AtomicBoolean superseded = new AtomicBoolean();
  private volatile boolean userCancelRequested;
  private final AtomicBoolean revoked = new AtomicBoolean();
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
      int baselineAttempts,
      long claimSeq,
      @Nullable String nodeId) {
    this.jobId = Objects.requireNonNull(jobId);
    this.deadline = Objects.requireNonNull(deadline);
    this.timeoutSeconds = timeoutSeconds;
    this.baselineAttempts = baselineAttempts;
    this.claimSeq = claimSeq;
    this.nodeId = nodeId;
    this.executionStartTime = Objects.requireNonNull(executionStartTime);
  }

  /**
   * Opaque identity of this attempt, recorded by CancellationRequestedException so an exception
   * kept from an earlier attempt is not taken as this attempt's cooperative stop.
   */
  public Object cancellationIdentity() {
    return cancellationIdentity;
  }

  /** Returns the persisted token when this attempt has a claim sequence. */
  public Optional<AttemptToken> token() {
    return claimSeq > 0 ? Optional.of(new AttemptToken(jobId, claimSeq)) : Optional.empty();
  }

  /**
   * Once set, this attempt is never current again. Called when an owner write is rejected or a
   * claim-fenced write returns SUPERSEDED.
   */
  public void markSuperseded() {
    superseded.set(true);
  }

  public boolean isSuperseded() {
    return superseded.get();
  }

  /**
   * Shutdown or the hard-timeout watchdog took this attempt from its worker; a bounced fenced write
   * only supersedes it.
   */
  public void revoke() {
    revoked.set(true);
    superseded.set(true);
  }

  public boolean isRevoked() {
    return revoked.get();
  }

  /** Reads whether this attempt still owns the live RUNNING claim. Store errors propagate. */
  public boolean isCurrent(JobClaimStore store) {
    if (claimSeq <= 0 || isSuperseded()) {
      return false;
    }
    Optional<ClaimState> claim = store.currentClaim(jobId);
    if (claim.isEmpty() || claim.get().claimSeq() != claimSeq) {
      markSuperseded();
      return false;
    }
    if (claim.get().status() != JobStatus.RUNNING) {
      return false;
    }
    return !isSuperseded();
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

  public long claimSeq() {
    return claimSeq;
  }

  public @Nullable String nodeId() {
    return nodeId;
  }

  public Instant executionStartTime() {
    return executionStartTime;
  }

  public void requestUserCancel() {
    userCancelRequested = true;
    requestCancellation();
  }

  public boolean isUserCancelRequested() {
    return userCancelRequested;
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
          revoke();
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
