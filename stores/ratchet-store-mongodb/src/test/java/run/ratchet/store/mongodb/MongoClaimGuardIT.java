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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/**
 * MongoDB has no {@code FOR UPDATE SKIP LOCKED}, so a job can be mutated between candidate
 * selection and the claim write. The claim write must re-assert the candidate predicate (here,
 * {@code scheduled_time <= now}) so a job rescheduled into the future in that window is not claimed
 * early.
 */
class MongoClaimGuardIT extends BaseDocumentStoreIT {

  @Test
  void rescheduleIntoFutureWithinClaimWindowBlocksClaim() {
    JobEntity job = newPendingJob();
    job.setScheduledTime(Instant.now().minusSeconds(5));
    job = store().save(job);
    UUID jobId = job.getId();

    AtomicBoolean fired = new AtomicBoolean(false);
    ((MongoJobStoreImpl) store())
        .setBeforeClaimWriteHook(
            () -> {
              if (fired.compareAndSet(false, true)) {
                // Concurrent reschedule: push the job an hour out, after it was selected as a
                // candidate but before the claim write commits.
                database()
                    .getCollection("scheduler_job")
                    .updateOne(
                        eq("_id", jobId),
                        set("scheduled_time", Date.from(Instant.now().plusSeconds(3600))));
              }
            });

    List<JobClaimDto> claimed =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1");

    assertTrue(claimed.isEmpty(), "rescheduled job must not be claimed inside the race window");
    JobEntity reloaded = store().findById(jobId).orElseThrow();
    assertEquals(
        JobStatus.PENDING, reloaded.getStatus(), "rescheduled job must stay PENDING, not RUNNING");
  }
}
