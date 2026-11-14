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

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

/** Shares live job cancellation while preserving keys held by running bodies. */
public final class LiveJobCancellation {
  private static final Logger log = Logger.getLogger(LiveJobCancellation.class);

  private LiveJobCancellation() {}

  /** Computes the business key hold deadline for a job, if it has a key. */
  public static @Nullable Instant keyHeldUntil(
      @Nullable JobEntity job, Instant now, RatchetOptions options) {
    if (job == null || job.getBusinessKey() == null) {
      return null;
    }
    long timeout =
        job.getTimeoutSec() > 0 ? job.getTimeoutSec() : options.timeout().defaultSlaSeconds();
    return now.plusSeconds(timeout).plusSeconds(options.node().orphanGraceSeconds());
  }

  /** Cancels a live job and returns the status it swapped from, or null if no swap won. */
  public static @Nullable JobStatus cancel(
      UUID jobId,
      @Nullable Instant keyHeldUntil,
      JobCrudStore crud,
      JobBatchStatusStore statuses,
      JobTerminalStore terminals) {
    for (int pass = 0; pass < 2; pass++) {
      for (JobStatus status :
          List.of(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PAUSED, JobStatus.WAITING)) {
        boolean won =
            status == JobStatus.RUNNING
                ? terminals.cancelRunningJob(jobId, keyHeldUntil)
                : statuses.compareAndSwapStatus(jobId, status, JobStatus.CANCELED, null);
        if (won) {
          log.debugf("Canceled job %s from %s", jobId, status);
          return status;
        }
      }
      if (pass == 0) {
        JobStatus current = crud.getJobStatus(jobId);
        if (current != JobStatus.PENDING
            && current != JobStatus.RUNNING
            && current != JobStatus.PAUSED
            && current != JobStatus.WAITING) break;
      }
    }
    return null;
  }
}
