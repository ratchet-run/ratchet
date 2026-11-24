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
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Submits the first outcome write to the release executor and retries through {@link
 * ReleaseRetries}. A RUNNING job on a node that still heartbeats has no other way to end, so a
 * failed write is retried with backoff until it lands, the row shows it is no longer this
 * attempt's, or the node stops.
 */
final class OutcomeWriteRetries {
  static final Duration FIRST_RETRY_DELAY = Duration.ofSeconds(5);
  static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(1);
  private static final Logger log = Logger.getLogger(OutcomeWriteRetries.class);
  private final ReleaseRetries retries;
  private final Supplier<ScheduledExecutorService> scheduler;

  OutcomeWriteRetries(
      int parallelism,
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer) {
    this(parallelism, scheduler, timer, System::nanoTime);
  }

  OutcomeWriteRetries(
      int parallelism,
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      LongSupplier nanoTime) {
    this.scheduler = scheduler;
    retries =
        new ReleaseRetries(
            parallelism,
            scheduler,
            timer,
            ReleaseRetries.Policy.untilReleased(FIRST_RETRY_DELAY, MAX_RETRY_DELAY),
            nanoTime,
            "outcome writes",
            "orphan recovery once this node stops heartbeating",
            Logger.Level.WARN);
  }

  /**
   * Submits the first try to the release executor. Returns false only when the node is stopping;
   * other refusals are logged and run inline so the outcome is not stranded.
   */
  boolean execute(String what, Runnable task) {
    ScheduledExecutorService executor = null;
    try {
      executor = scheduler.get();
      executor.execute(task);
      return true;
    } catch (RuntimeException failure) {
      if (ReleaseRetries.isStopping(failure, executor)) return false;
      log.warnf(failure, "Could not execute %s on the release executor; running inline", what);
    }
    task.run();
    return true;
  }

  boolean run(String what, Runnable write) {
    return retries.release(
        new ReleaseRetries.Release(
            new Object(),
            what,
            "orphan recovery once this node stops heartbeating",
            Logger.Level.WARN,
            write));
  }
}
