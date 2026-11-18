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
 * Retries an outcome write through {@link ReleaseRetries}. A RUNNING job on a node that still
 * heartbeats has no other way to end, so a failed write is retried with backoff until it lands, the
 * row shows it is no longer this attempt's, or the node stops.
 */
final class OutcomeWriteRetries {
  static final Duration FIRST_RETRY_DELAY = Duration.ofSeconds(5);
  static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(1);
  private final ReleaseRetries retries;

  OutcomeWriteRetries(
      Supplier<ScheduledExecutorService> scheduler, Supplier<ScheduledExecutorService> timer) {
    this(scheduler, timer, System::nanoTime);
  }

  OutcomeWriteRetries(
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      LongSupplier nanoTime) {
    retries =
        new ReleaseRetries(
            scheduler,
            timer,
            ReleaseRetries.Policy.untilReleased(FIRST_RETRY_DELAY, MAX_RETRY_DELAY),
            nanoTime);
  }

  boolean run(String what, Runnable write) {
    return retries.release(
        new ReleaseRetries.Release(
            what, "orphan recovery once this node stops heartbeating", Logger.Level.WARN, write));
  }
}
