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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Runs a store release that an ended attempt owes. A thread that is still interrupted must not call
 * the store, because a JDBC call on an interrupted thread can break the pooled connection, so the
 * release goes to a scheduler thread at once. A failed store call is retried there every {@link
 * #RETRY_DELAY}, up to {@link #MAX_RETRIES} times. Nothing here throws to the caller.
 */
final class ReleaseRetries {
  static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  static final int MAX_RETRIES = 60;
  private static final Logger log = Logger.getLogger(ReleaseRetries.class);

  /**
   * One release. {@code what} names it in log messages, {@code leftTo} names what frees it once the
   * retries stop, and {@code unscheduledLevel} is the level of the message logged when the
   * scheduler does not take it.
   */
  record Release(String what, String leftTo, Logger.Level unscheduledLevel, Runnable call) {}

  private final Supplier<ScheduledExecutorService> scheduler;

  ReleaseRetries(Supplier<ScheduledExecutorService> scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Runs the release on this thread, or hands it to the scheduler when this thread is interrupted
   * or the call fails.
   *
   * @return true when the release ran on this thread
   */
  boolean release(Release release) {
    if (Thread.currentThread().isInterrupted()) {
      // The interrupt stays set; the caller restores or handles it.
      schedule(release, 1, 0);
      return false;
    }
    try {
      release.call().run();
      return true;
    } catch (RuntimeException e) {
      log.warnf(e, "Could not release %s; scheduling retry", release.what());
      schedule(release, 1, RETRY_DELAY.toMillis());
      return false;
    }
  }

  private void schedule(Release release, int attempt, long delayMillis) {
    try {
      scheduler.get().schedule(() -> retry(release, attempt), delayMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      log.logf(
          release.unscheduledLevel(),
          e,
          "Could not schedule release of %s; left to %s",
          release.what(),
          release.leftTo());
    }
  }

  private void retry(Release release, int attempt) {
    try {
      release.call().run();
    } catch (RuntimeException e) {
      log.debugf(e, "Release attempt %s failed for %s", attempt, release.what());
      if (attempt < MAX_RETRIES) {
        schedule(release, attempt + 1, RETRY_DELAY.toMillis());
      } else {
        log.warnf(
            "Gave up releasing %s after %s retries; left to %s",
            release.what(), attempt, release.leftTo());
      }
    }
  }
}
