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
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.internal.ExecutorProviderClosedException;

/**
 * Runs a store write that an attempt owes, including releases and outcome writes. A thread that is
 * still interrupted must not call the store, because a JDBC call on an interrupted thread can break
 * the pooled connection, so the write goes to a release executor thread at once. A failed store
 * call is retried there as the caller's {@link Policy} says, until the write lands or this node
 * stops for an unbounded policy with a resubmission timer. Nothing here throws to the caller.
 */
final class ReleaseRetries {
  private static final Logger log = Logger.getLogger(ReleaseRetries.class);
  static final Duration WARN_INTERVAL = Duration.ofMinutes(10);

  /**
   * One write. {@code what} names it in log messages, {@code leftTo} names what finishes it once
   * the retries stop, and {@code unscheduledLevel} is the level of the message logged when the
   * release executor does not take it.
   */
  record Release(String what, String leftTo, Logger.Level unscheduledLevel, Runnable call) {}

  /**
   * How a failed write is retried. The first retry waits {@code firstDelay} and each later one
   * waits twice as long as the one before, up to {@code maxDelay}. A policy with {@code maxRetries}
   * stops after that many retries. A policy without it retries until the write lands or this node
   * stops when a resubmission timer is present; because nothing ends such a write on its own, a
   * failed retry warns again once {@link #WARN_INTERVAL} has passed on the clock since the last
   * warning, and the write logs at INFO when it lands.
   */
  record Policy(Duration firstDelay, Duration maxDelay, OptionalInt maxRetries) {
    Policy {
      Objects.requireNonNull(firstDelay);
      Objects.requireNonNull(maxDelay);
      Objects.requireNonNull(maxRetries);
      if (firstDelay.isZero() || firstDelay.isNegative()) {
        throw new IllegalArgumentException("firstDelay must be positive");
      }
      if (maxDelay.compareTo(firstDelay) < 0) {
        throw new IllegalArgumentException("maxDelay must not be below firstDelay");
      }
      if (maxRetries.isPresent() && maxRetries.getAsInt() < 1) {
        throw new IllegalArgumentException("maxRetries must be at least 1");
      }
    }

    /** Retries at a fixed delay up to the given number of retries. */
    static Policy bounded(Duration delay, int maxRetries) {
      return new Policy(delay, delay, OptionalInt.of(maxRetries));
    }

    /** Retries with increasing delays until the write lands or this node stops. */
    static Policy untilReleased(Duration firstDelay, Duration maxDelay) {
      return new Policy(firstDelay, maxDelay, OptionalInt.empty());
    }
  }

  /**
   * One scheduled try. {@code number} counts the scheduled tries of this write, {@code delayMillis}
   * is this try's delay, {@code warned} says whether a warning was logged for this write, and
   * {@code warnedAtNanos} is the clock reading at that warning. Refusals have their own backoff,
   * reset on acceptance, and share the warning clock once the first refusal is logged.
   */
  private record Retry(
      int number,
      long delayMillis,
      boolean warned,
      long warnedAtNanos,
      long refusalDelayMillis,
      boolean refusalWarned) {
    Retry(int number, long delayMillis, boolean warned, long warnedAtNanos) {
      this(number, delayMillis, warned, warnedAtNanos, 0, false);
    }
  }

  private final Supplier<ScheduledExecutorService> scheduler;
  private final Supplier<ScheduledExecutorService> timer;
  private final Policy policy;
  private final LongSupplier nanoTime;

  /**
   * @param nanoTime a monotonic clock in nanoseconds, such as {@code System::nanoTime}
   */
  ReleaseRetries(
      Supplier<ScheduledExecutorService> scheduler, Policy policy, LongSupplier nanoTime) {
    this(scheduler, null, policy, nanoTime);
  }

  /**
   * @param timer optional timer for resubmission; null leaves refused writes to recovery
   */
  ReleaseRetries(
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      Policy policy,
      LongSupplier nanoTime) {
    this.scheduler = scheduler;
    this.timer = timer;
    this.policy = policy;
    this.nanoTime = nanoTime;
  }

  /**
   * Runs the write on this thread, or hands it to the release executor when this thread is
   * interrupted or the call fails.
   *
   * @return true when the write ran on this thread
   */
  boolean release(Release release) {
    if (Thread.currentThread().isInterrupted()) {
      // The interrupt stays set; the caller restores or handles it.
      schedule(release, new Retry(1, 0, false, 0));
      return false;
    }
    try {
      release.call().run();
      return true;
    } catch (RuntimeException e) {
      log.warnf(e, "Could not finish %s; scheduling retry", release.what());
      long first = policy.firstDelay().toMillis();
      schedule(release, new Retry(1, first, true, nanoTime.getAsLong()));
      return false;
    }
  }

  private void schedule(Release release, Retry retry) {
    ScheduledExecutorService executor = null;
    try {
      executor = scheduler.get();
      Retry accepted =
          new Retry(
              retry.number(),
              retry.delayMillis(),
              retry.warned(),
              retry.warnedAtNanos(),
              0,
              retry.refusalWarned());
      executor.schedule(() -> retry(release, accepted), retry.delayMillis(), TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      if (!isStopping(e, executor) && timer != null && policy.maxRetries().isEmpty()) {
        resubmit(release, retry, e);
        return;
      }
      logUnscheduled(release, e, executor);
    }
  }

  private void resubmit(Release release, Retry retry, RuntimeException refusal) {
    long delay =
        retry.refusalDelayMillis() == 0
            ? policy.firstDelay().toMillis()
            : Math.min(retry.refusalDelayMillis() * 2, policy.maxDelay().toMillis());
    long now = nanoTime.getAsLong();
    boolean warn = !retry.refusalWarned() || now - retry.warnedAtNanos() >= WARN_INTERVAL.toNanos();
    Retry next =
        new Retry(
            retry.number(),
            retry.delayMillis(),
            retry.warned() || warn,
            warn ? now : retry.warnedAtNanos(),
            delay,
            true);
    ScheduledExecutorService executor = null;
    try {
      executor = timer.get();
      // A resubmission firing after shutdown is refused with isShutdown and logs stopping once.
      // Only scheduling runs on the timer thread, never the release's store call.
      executor.schedule(() -> schedule(release, next), delay, TimeUnit.MILLISECONDS);
    } catch (RuntimeException failure) {
      logUnscheduled(release, failure, executor);
      return;
    }
    if (warn) {
      log.logf(
          release.unscheduledLevel(),
          refusal,
          "Could not schedule %s; trying again in %s seconds",
          release.what(),
          delay / 1000);
    }
  }

  private void logUnscheduled(
      Release release, RuntimeException e, ScheduledExecutorService executor) {
    if (isStopping(e, executor)) {
      log.infof(e, "This node is stopping; %s left to %s", release.what(), release.leftTo());
    } else {
      log.logf(
          release.unscheduledLevel(),
          e,
          "Could not schedule %s; left to %s",
          release.what(),
          release.leftTo());
    }
  }

  static boolean isStopping(RuntimeException failure, ScheduledExecutorService executor) {
    if (failure instanceof ExecutorProviderClosedException) return true;
    if (!(failure instanceof RejectedExecutionException) || executor == null) return false;
    try {
      return executor.isShutdown();
    } catch (RuntimeException unavailable) {
      return false;
    }
  }

  private void retry(Release release, Retry retry) {
    try {
      release.call().run();
      if (policy.maxRetries().isEmpty() && retry.warned()) {
        log.infof("Finished %s on retry %s", release.what(), retry.number());
      }
    } catch (RuntimeException e) {
      long first = policy.firstDelay().toMillis();
      long max = policy.maxDelay().toMillis();
      long next = retry.delayMillis() == 0 ? first : Math.min(retry.delayMillis() * 2, max);
      if (policy.maxRetries().isPresent()) {
        log.debugf(e, "Attempt %s failed for %s", retry.number(), release.what());
        if (retry.number() < policy.maxRetries().getAsInt()) {
          schedule(
              release, new Retry(retry.number() + 1, next, retry.warned(), retry.warnedAtNanos()));
        } else {
          log.warnf(
              "Gave up on %s after %s retries; left to %s",
              release.what(), retry.number(), release.leftTo());
        }
      } else {
        retryUntilReleased(release, retry, next, e);
      }
    }
  }

  private void retryUntilReleased(Release release, Retry retry, long next, RuntimeException e) {
    // Time on the clock, so a store call that hangs or a retry the release executor runs late
    // counts too.
    long now = nanoTime.getAsLong();
    if (!retry.warned()) {
      log.warnf(e, "Could not finish %s; scheduling retry", release.what());
      schedule(release, new Retry(retry.number() + 1, next, true, now, 0, retry.refusalWarned()));
    } else if (now - retry.warnedAtNanos() >= WARN_INTERVAL.toNanos()) {
      log.warnf(
          e,
          "Still could not finish %s after %s retries; retrying every %s seconds until it lands or"
              + " this node stops",
          release.what(),
          retry.number(),
          next / 1000);
      schedule(release, new Retry(retry.number() + 1, next, true, now, 0, retry.refusalWarned()));
    } else {
      log.debugf(e, "Attempt %s failed for %s", retry.number(), release.what());
      schedule(
          release,
          new Retry(
              retry.number() + 1, next, true, retry.warnedAtNanos(), 0, retry.refusalWarned()));
    }
  }
}
