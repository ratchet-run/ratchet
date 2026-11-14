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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Releases the business-key hold of a cancelled attempt. A worker thread that was interrupted, for
 * example by the watchdog at the deadline, must not call the store, because a JDBC call on an
 * interrupted thread can break the pooled connection. A store call can also fail. In both cases the
 * release moves to a scheduler thread and is retried there. This is a best effort: the hold's
 * expiry, the stale-node release and the start-up release are the backstops.
 */
final class HeldKeyReleases {
  static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  static final int MAX_RETRIES = 60;
  private static final Logger log = Logger.getLogger(HeldKeyReleases.class);
  private final JobTerminalStore store;
  private final Supplier<ScheduledExecutorService> scheduler;

  HeldKeyReleases(JobTerminalStore store, Supplier<ScheduledExecutorService> scheduler) {
    this.store = store;
    this.scheduler = scheduler;
  }

  /** Releases the hold of this claim. */
  void release(UUID jobId, long claimSeq) {
    if (Thread.currentThread().isInterrupted()) {
      schedule(jobId, claimSeq, 1, 0);
      return;
    }
    try {
      store.releaseBusinessKeyHold(jobId, claimSeq);
    } catch (RuntimeException e) {
      log.warnf(e, "Could not release business-key hold for job %s; scheduling retry", jobId);
      schedule(jobId, claimSeq, 1, RETRY_DELAY.toMillis());
    }
  }

  private void schedule(UUID jobId, long claimSeq, int attempt, long delayMillis) {
    try {
      scheduler
          .get()
          .schedule(() -> retry(jobId, claimSeq, attempt), delayMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      log.debugf(e, "Could not schedule business-key hold release for job %s", jobId);
    }
  }

  private void retry(UUID jobId, long claimSeq, int attempt) {
    try {
      store.releaseBusinessKeyHold(jobId, claimSeq);
    } catch (RuntimeException e) {
      log.debugf(e, "Business-key hold release attempt %s failed for job %s", attempt, jobId);
      if (attempt < MAX_RETRIES) {
        schedule(jobId, claimSeq, attempt + 1, RETRY_DELAY.toMillis());
      } else {
        log.warnf(
            "Business-key hold for job %s left to its expiry after %s attempts", jobId, attempt);
      }
    }
  }
}
