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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;

/**
 * Regression coverage for the archive scheduled-time field. Archive documents persist the schedule
 * as {@code original_scheduled_time}; the query layer must apply {@link
 * JobFilter#scheduledAfter()}/{@link JobFilter#scheduledBefore()} against that field, not the live
 * {@code scheduled_time} the archive document lacks.
 */
class MongoArchiveScheduledTimeFilterIT extends BaseDocumentStoreIT {

  @Test
  void includeArchivedHonorsScheduledTimeBoundsAgainstOriginalScheduledTime() {
    Instant scheduled = Instant.now().truncatedTo(ChronoUnit.MILLIS).minus(2, ChronoUnit.HOURS);

    JobEntity job = newPendingJob();
    job.setScheduledTime(scheduled);
    job = complete(store().save(job));

    int archived = store().archiveJobsBatch(List.of(job), "retention", "system");
    assertEquals(1, archived);
    assertTrue(store().findById(job.getId()).isEmpty(), "live job should be gone after archiving");

    JobFilter inWindow =
        JobFilter.builder()
            .includeArchived(true)
            .scheduledAfter(scheduled.minus(1, ChronoUnit.HOURS))
            .scheduledBefore(scheduled.plus(1, ChronoUnit.HOURS))
            .build();
    List<JobEntity> hits = store().searchJobs(inWindow, 50, 0);
    assertEquals(1, hits.size(), "archived job within the scheduled-time window must be returned");
    assertEquals(job.getId(), hits.get(0).getId());

    JobFilter outsideWindow =
        JobFilter.builder()
            .includeArchived(true)
            .scheduledAfter(scheduled.plus(1, ChronoUnit.HOURS))
            .build();
    assertTrue(
        store().searchJobs(outsideWindow, 50, 0).isEmpty(),
        "archived job outside the scheduled-time window must be excluded");
  }

  private JobEntity complete(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    return store().findById(job.getId()).orElseThrow();
  }
}
