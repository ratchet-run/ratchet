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
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Releases the business-key hold of a cancelled attempt through {@link ReleaseRetries}. The worker
 * clears the interrupt that the watchdog or a shutdown gives a running body before it gets here,
 * but a thread that is still interrupted must not call the store, because a JDBC call on an
 * interrupted thread can break the pooled connection. A store call can also fail. In both cases the
 * release moves to a release executor thread and is retried there. This is a best effort: the
 * hold's expiry, the stale-node release and the start-up release are the backstops.
 */
final class HeldKeyReleases {
  static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  static final int MAX_RETRIES = 60;
  private final JobTerminalStore store;
  private final ReleaseRetries retries;

  HeldKeyReleases(
      int parallelism, JobTerminalStore store, Supplier<ScheduledExecutorService> scheduler) {
    this(parallelism, store, scheduler, System::nanoTime);
  }

  HeldKeyReleases(
      int parallelism,
      JobTerminalStore store,
      Supplier<ScheduledExecutorService> scheduler,
      LongSupplier clock) {
    this.store = store;
    this.retries =
        new ReleaseRetries(
            parallelism,
            scheduler,
            null,
            ReleaseRetries.Policy.bounded(RETRY_DELAY, MAX_RETRIES),
            clock,
            "business-key hold releases",
            "its expiry",
            Logger.Level.DEBUG);
  }

  /** Releases the hold of this claim. */
  void release(UUID jobId, long claimSeq) {
    retries.release(
        new ReleaseRetries.Release(
            new Key(jobId, claimSeq),
            "release of the business-key hold for job " + jobId,
            "its expiry",
            Logger.Level.DEBUG,
            () -> store.releaseBusinessKeyHold(jobId, claimSeq)));
  }

  private record Key(UUID jobId, long claimSeq) {}
}
