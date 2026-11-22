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
  record Release(
      Object key, String what, String leftTo, Logger.Level unscheduledLevel, Runnable call) {}

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

  private static final class Retry {
    int number = 1;
    long delayMillis;
    boolean warned;
    long warnedAtNanos;
  }

  private final Policy policy;
  private final LongSupplier nanoTime;
  private final RetryDrain drain;

  ReleaseRetries(
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      Policy policy,
      LongSupplier nanoTime,
      String kind,
      String leftTo,
      Logger.Level level) {
    this.policy = policy;
    this.nanoTime = nanoTime;
    drain =
        new RetryDrain(
            kind,
            scheduler,
            policy.maxRetries().isEmpty() ? timer : null,
            nanoTime,
            policy.firstDelay(),
            policy.maxDelay(),
            new RetryDrain.Refusal() {
              public void dropped(int count, boolean stopping, RuntimeException failure) {
                if (stopping)
                  log.infof(
                      failure, "This node is stopping; %s %s left to %s", count, kind, leftTo);
                else
                  log.logf(
                      level, failure, "Could not schedule %s %s; left to %s", count, kind, leftTo);
              }

              public void retrying(long delayNanos, RuntimeException failure) {
                log.logf(
                    level,
                    failure,
                    "Could not schedule %s; trying again in %s seconds",
                    kind,
                    TimeUnit.NANOSECONDS.toSeconds(delayNanos));
              }
            });
  }

  boolean release(Release release) {
    RetryDrain.Entry entry = drain.reserve(release.key(), () -> {});
    if (entry == null) {
      log.debugf("%s is already pending", release.what());
      return false;
    }
    Retry retry = new Retry();
    if (!Thread.currentThread().isInterrupted()) {
      try {
        release.call().run();
        drain.finish(entry);
        return true;
      } catch (RuntimeException failure) {
        log.warnf(failure, "Could not finish %s; scheduling retry", release.what());
        retry.delayMillis = policy.firstDelay().toMillis();
        retry.warned = true;
        retry.warnedAtNanos = nanoTime.getAsLong();
      } catch (Error failure) {
        drain.finish(entry);
        throw failure;
      }
    }
    drain.schedule(
        entry,
        nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(retry.delayMillis),
        () -> retry(release, retry),
        (stopping, failure) -> {
          if (stopping)
            log.infof(
                failure, "This node is stopping; %s left to %s", release.what(), release.leftTo());
          else
            log.logf(
                release.unscheduledLevel(),
                failure,
                "Could not schedule %s; left to %s",
                release.what(),
                release.leftTo());
        },
        (delay, failure) -> {
          log.logf(
              release.unscheduledLevel(),
              failure,
              "Could not schedule %s; trying again in %s seconds",
              release.what(),
              TimeUnit.NANOSECONDS.toSeconds(delay));
          retry.warned = true;
          retry.warnedAtNanos = nanoTime.getAsLong();
        });
    return false;
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

  private Long retry(Release release, Retry retry) {
    try {
      release.call().run();
      if (policy.maxRetries().isEmpty() && retry.warned) {
        log.infof("Finished %s on retry %s", release.what(), retry.number);
      }
      return null;
    } catch (RuntimeException failure) {
      long next =
          retry.delayMillis == 0
              ? policy.firstDelay().toMillis()
              : Math.min(retry.delayMillis * 2, policy.maxDelay().toMillis());
      long now = nanoTime.getAsLong();
      if (policy.maxRetries().isPresent()) {
        log.debugf(failure, "Attempt %s failed for %s", retry.number, release.what());
        if (retry.number >= policy.maxRetries().getAsInt()) {
          log.warnf(
              "Gave up on %s after %s retries; left to %s",
              release.what(), retry.number, release.leftTo());
          return null;
        }
      } else if (!retry.warned) {
        log.warnf(failure, "Could not finish %s; scheduling retry", release.what());
        retry.warned = true;
        retry.warnedAtNanos = now;
      } else if (now - retry.warnedAtNanos >= WARN_INTERVAL.toNanos()) {
        log.warnf(
            failure,
            "Still could not finish %s after %s retries; retrying every %s seconds until it lands"
                + " or this node stops",
            release.what(),
            retry.number,
            next / 1000);
        retry.warnedAtNanos = now;
      } else {
        log.debugf(failure, "Attempt %s failed for %s", retry.number, release.what());
      }
      retry.number++;
      retry.delayMillis = next;
      return nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(next);
    }
  }
}
