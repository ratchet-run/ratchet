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
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.ResourcePermitService;

/**
 * Releases the resource permit of an ended attempt through {@link ReleaseRetries}. The release goes
 * through {@link ResourcePermitService}, so a release that lands on a retry also wakes the
 * resource's waiters, and it does nothing when the store has no ResourcePermitStore. Each call
 * names the attempt's claim, so a retry never removes a permit that a newer attempt of the same job
 * holds. A permit has no expiry, so a failed release is retried with backoff, from {@link
 * #FIRST_RETRY_DELAY} up to {@link #MAX_RETRY_DELAY} between tries, until it lands or the node
 * stops. A permit still held when the release executor refuses a retry, as it does once the node
 * stops, stays until this node's start-up clean-up or, after its heartbeat stops, another node's
 * stale-node clean-up removes it.
 */
final class PermitReleases {
  static final Duration FIRST_RETRY_DELAY = Duration.ofSeconds(5);
  static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(1);
  private final ResourcePermitService permits;
  private final ReleaseRetries retries;

  PermitReleases(ResourcePermitService permits, Supplier<ScheduledExecutorService> scheduler) {
    this(permits, scheduler, System::nanoTime);
  }

  /** Takes the monotonic clock that times the repeated warnings, so tests can move it. */
  PermitReleases(
      ResourcePermitService permits,
      Supplier<ScheduledExecutorService> scheduler,
      LongSupplier nanoTime) {
    this.permits = permits;
    this.retries =
        new ReleaseRetries(
            scheduler,
            ReleaseRetries.Policy.untilReleased(FIRST_RETRY_DELAY, MAX_RETRY_DELAY),
            nanoTime);
  }

  /**
   * Releases the permit this claim holds on the resource. Never throws for a failed store call.
   *
   * @return true when the permit was released on this thread; false when the release went to the
   *     release executor
   */
  boolean release(String resource, UUID jobId, long claimSeq) {
    return retries.release(
        new ReleaseRetries.Release(
            "resource permit '%s' of job %s claim %s".formatted(resource, jobId, claimSeq),
            "node clean-up after this node stops or restarts",
            Logger.Level.WARN,
            () -> permits.release(resource, jobId, claimSeq)));
  }
}
