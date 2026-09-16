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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;

/** Persists successful execution using bounded retries and a result-free fallback. */
@ApplicationScoped
public class JobSuccessFinalizer {

  private static final Logger log = Logger.getLogger(JobSuccessFinalizer.class);
  private static final int MAX_ATTEMPTS = 5;
  private static final long[] BACKOFF_MS = {25L, 50L, 100L, 200L, 400L};
  private static final long JITTER_MAX_MS = 25L;

  private final PostExecutionHandler lifecycle;
  private final ExecutionObserver observer;
  private final Sleeper sleeper;
  private final LongSupplier jitter;

  protected JobSuccessFinalizer() {
    this.lifecycle = null;
    this.observer = null;
    this.sleeper = null;
    this.jitter = null;
  }

  @Inject
  public JobSuccessFinalizer(PostExecutionHandler lifecycle, ExecutionObserver observer) {
    this(
        lifecycle,
        observer,
        Thread::sleep,
        () -> ThreadLocalRandom.current().nextLong(JITTER_MAX_MS + 1L));
  }

  JobSuccessFinalizer(
      PostExecutionHandler lifecycle,
      ExecutionObserver observer,
      Sleeper sleeper,
      LongSupplier jitter) {
    this.lifecycle = lifecycle;
    this.observer = observer;
    this.sleeper = sleeper;
    this.jitter = jitter;
  }

  /**
   * Persists full success, falling back to minimal success after transient conflicts are exhausted.
   */
  public Outcome finalizeSuccess(
      JobEntity job,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs) {
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        boolean updated =
            lifecycle.completeSuccess(
                job, resultJson, resultType, start, end, durationMs, queueWaitMs);
        return updated ? Outcome.COMPLETED_FULL : Outcome.TERMINAL_SKIPPED;
      } catch (RatchetTransientStoreException e) {
        observer.recordSuccessFinalizationRetry(job);
        if (attempt == MAX_ATTEMPTS) {
          log.warnf(
              "Job %s exhausted success finalization retries after transient store conflicts: %s",
              job.getId(), e.getMessage());
          break;
        }

        log.warnf(
            "Job %s transient success finalization failure on attempt %s/%s: %s",
            job.getId(), attempt, MAX_ATTEMPTS, e.getMessage());
        if (!sleepBeforeRetry(job, attempt)) {
          break;
        }
      }
    }

    try {
      boolean updated = lifecycle.completeSuccessMinimal(job, start, end, durationMs, queueWaitMs);
      if (updated) {
        observer.recordSuccessFinalizationMinimal(job);
        log.warnf(
            "Job %s persisted minimal success after transient store finalization conflicts",
            job.getId());
        return Outcome.COMPLETED_MINIMAL;
      }
      return Outcome.TERMINAL_SKIPPED;
    } catch (RatchetTransientStoreException e) {
      observer.recordSuccessFinalizationStuck(job);
      log.errorf(
          e,
          "Job %s succeeded but success finalization is stuck after transient store conflicts",
          job.getId());
      return Outcome.STUCK;
    }
  }

  private boolean sleepBeforeRetry(JobEntity job, int attempt) {
    long baseDelay = BACKOFF_MS[Math.min(attempt - 1, BACKOFF_MS.length - 1)];
    long delay = baseDelay + jitter.getAsLong();
    try {
      sleeper.sleep(delay);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warnf("Job %s finalization retry interrupted", job.getId());
      return false;
    }
  }

  public enum Outcome {
    COMPLETED_FULL,
    COMPLETED_MINIMAL,
    TERMINAL_SKIPPED,
    STUCK
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long delayMs) throws InterruptedException;
  }
}
