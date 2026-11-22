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

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.store.spi.JobRetryStore;

/**
 * Retries, on a release executor thread, a claim release that a worker could not write. A worker
 * gives back a claim it will not run (after a transient permit failure, a superseded permit, an
 * open circuit, an envelope upgrade, a failed batch check or a completed continuation) through
 * {@link JobRetryStore#releaseClaim}. If the store keeps failing, the job would stay RUNNING with
 * no worker: the timeout watchdog stops when the worker returns, and orphan recovery skips nodes
 * that still heartbeat.
 *
 * <p>Pending writes are keyed by job and claim sequence. Each write is fenced on the claim sequence
 * and attempt count, so a charge committed under the same claim always wins. No write charges an
 * attempt. While this node runs, each hand-off that adds an entry ends in exactly one accepted,
 * rejected or dropped callback. A hand-off for a job and claim that already has an entry adds
 * nothing, and the first entry's callbacks run. A dropped write has no store result, including when
 * the watchdog revokes it. There is one entry per ended attempt whose write still fails, so at most
 * the attempts this node ended during the outage. Entries are dropped when revoked, after a
 * non-transient store failure, or when both executors refuse. If this node stops with a drain still
 * scheduled, its entries run no callback. Shutdown reset or orphan recovery handles unwritten rows,
 * and orphan recovery closes their stale execution rows. The revoked check only avoids a write the
 * timeout watchdog has taken over. Refused release tasks are resubmitted from the scheduled
 * executor while this node runs.
 */
class ClaimReschedules {
  private static final Logger log = Logger.getLogger(ClaimReschedules.class);
  private final JobRetryStore store;
  private final LongSupplier clock;
  private final RetryDrain drain;

  ClaimReschedules(
      JobRetryStore store,
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer) {
    this(store, scheduler, timer, System::nanoTime);
  }

  ClaimReschedules(
      JobRetryStore store,
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      LongSupplier clock) {
    this.store = store;
    this.clock = clock;
    drain =
        new RetryDrain(
            "claim reschedules",
            scheduler,
            timer,
            clock,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            new RetryDrain.Refusal() {
              public void dropped(int count, boolean stopping, RuntimeException failure) {
                if (stopping)
                  log.infof(
                      "This node is stopping; %s claim reschedules left to this node's shutdown"
                          + " reset or orphan recovery",
                      count);
                else
                  log.warnf(
                      "Could not re-arm claim reschedules; %s pending items pass to this node's"
                          + " shutdown reset or orphan recovery",
                      count);
              }

              public void retrying(long delayNanos, RuntimeException failure) {
                log.warnf(
                    failure,
                    "Could not schedule claim reschedules; trying again in %s seconds",
                    TimeUnit.NANOSECONDS.toSeconds(delayNanos));
              }
            });
  }

  boolean handOff(
      UUID jobId,
      long claimSeq,
      String reason,
      Instant scheduledTime,
      int attempts,
      BooleanSupplier revoked,
      Runnable afterAccepted,
      Runnable afterRejected,
      Runnable afterDropped) {
    Key key = new Key(jobId, claimSeq);
    Pending item =
        new Pending(
            reason,
            scheduledTime,
            attempts,
            revoked,
            afterAccepted,
            afterRejected,
            afterDropped,
            clock.getAsLong());
    RetryDrain.Submission result =
        drain.submit(
            key,
            item.due,
            entry -> () -> tryItem(key, item, entry),
            () -> runCallback(key, afterDropped, "Dropped"));
    if (result == RetryDrain.Submission.DUPLICATE) return true;
    if (result == RetryDrain.Submission.REFUSED) return false;
    log.warnf(
        "Job %s claim %s could not be rescheduled; retrying in the background", jobId, claimSeq);
    return true;
  }

  private Long tryItem(Key key, Pending item, RetryDrain.Entry entry) {
    long now = clock.getAsLong();
    boolean revoked = false;
    try {
      revoked = item.revoked.getAsBoolean();
    } catch (RuntimeException failure) {
      log.debugf(
          failure, "Could not check revocation for job %s claim %s", key.jobId(), key.claimSeq());
    }
    if (revoked) {
      drain.finish(entry);
      runCallback(key, item.afterDropped, "Dropped");
      log.infof(
          "Job %s claim %s reschedule belongs to the timeout watchdog",
          key.jobId(), key.claimSeq());
      return null;
    }
    item.tries++;
    try {
      boolean accepted =
          store.releaseClaim(
              key.jobId(), item.reason, item.scheduledTime, item.attempts, key.claimSeq());
      drain.finish(entry);
      if (accepted) {
        log.infof(
            "Job %s claim %s rescheduled after %s background tries",
            key.jobId(), key.claimSeq(), item.tries);
        runCallback(key, item.afterAccepted, "Accepted");
      } else {
        runCallback(key, item.afterRejected, "Rejected");
      }
    } catch (RuntimeException failure) {
      if (!JobSuccessFinalizer.isTransientStoreFailure(failure)) {
        drain.finish(entry);
        runCallback(key, item.afterDropped, "Dropped");
        log.errorf(
            failure,
            "Job %s claim %s stays RUNNING until this node's shutdown reset or,"
                + " after its heartbeat stops, orphan recovery",
            key.jobId(),
            key.claimSeq());
        return null;
      }
      // The first background try runs a second after hand-off. After it, tries run five
      // seconds after hand-off and then 10, 20, 40 and 60 seconds apart, with no limit.
      item.due =
          item.tries == 1
              ? item.started + TimeUnit.SECONDS.toNanos(5)
              : now + TimeUnit.SECONDS.toNanos(item.delaySeconds);
      if (item.tries > 1) item.delaySeconds = Math.min(60, item.delaySeconds * 2);
      log.debugf(failure, "Could not reschedule job %s claim %s", key.jobId(), key.claimSeq());
      if (now - item.lastWarn >= TimeUnit.MINUTES.toNanos(10)) {
        item.lastWarn = now;
        log.warnf(
            failure, "Job %s claim %s still could not be rescheduled", key.jobId(), key.claimSeq());
      }
      return item.due;
    }
    return null;
  }

  private void runCallback(Key key, Runnable callback, String outcome) {
    try {
      callback.run();
    } catch (RuntimeException failure) {
      log.warnf(
          failure,
          "%s reschedule callback failed for job %s claim %s",
          outcome,
          key.jobId(),
          key.claimSeq());
    }
  }

  private record Key(UUID jobId, long claimSeq) {}

  private static final class Pending {
    final String reason;
    final Instant scheduledTime;
    final int attempts;
    final BooleanSupplier revoked;
    final Runnable afterAccepted;
    final Runnable afterRejected;
    final Runnable afterDropped;
    final long started;
    long due;
    long lastWarn;
    long delaySeconds = 10;
    int tries;

    Pending(
        String reason,
        Instant scheduledTime,
        int attempts,
        BooleanSupplier revoked,
        Runnable afterAccepted,
        Runnable afterRejected,
        Runnable afterDropped,
        long now) {
      this.reason = reason;
      this.scheduledTime = scheduledTime;
      this.attempts = attempts;
      this.revoked = revoked;
      this.afterAccepted = afterAccepted;
      this.afterRejected = afterRejected;
      this.afterDropped = afterDropped;
      this.started = now;
      this.due = now + TimeUnit.SECONDS.toNanos(1);
      this.lastWarn = now;
    }
  }
}
