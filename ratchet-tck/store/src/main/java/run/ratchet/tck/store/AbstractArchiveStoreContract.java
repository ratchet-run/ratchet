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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;

/** Base contract tests for {@code ArchiveStore}. */
public abstract class AbstractArchiveStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupArchiveFixture() {
    cleanupStore();
  }

  @Test
  void archiveJob_createsArchiveRecord() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    // Fixed instants avoid wall-clock resolution hazards on fast CI machines where two
    // Instant.now() calls within the same millisecond produce equal start/end times.
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived = archiveStore().archiveJob(completed, "test", "tck");

    assertNotNull(archived, "archiveJob should return a non-null archived entity");
    assertEquals(
        completed.getId(),
        archived.getOriginalJobId(),
        "Archived entity should reference the original job ID");
  }

  @Test
  void findArchivedJobs_returnsByTargetClass() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();
    archiveStore().archiveJob(completed, "test", "tck");

    var results =
        archiveStore().findArchivedJobs(completed.getPayload().target(), null, null, null, 10);

    assertFalse(results.isEmpty(), "findArchivedJobs should return the archived job");
    assertEquals(
        completed.getId(),
        results.get(0).getOriginalJobId(),
        "Archived result should match the original job");
  }

  @Test
  void archiveJobsBatch_archivesMultipleJobs() {
    var job1 = completeJob(persist(newPendingJob()));
    var job2 = completeJob(persist(newPendingJob()));

    int count = archiveStore().archiveJobsBatch(List.of(job1, job2), "batch-test", "tck");

    assertEquals(2, count, "archiveJobsBatch should archive both jobs");
  }

  @Test
  void findJobsForArchiving_excludesRecentJobs() {
    persist(newPendingJob());

    var candidates = archiveStore().findJobsForArchiving(Instant.now().minusSeconds(3600), 10);

    assertTrue(
        candidates.isEmpty(),
        "A job created just now should not be eligible for archiving with a 1-hour cutoff");
  }

  @Test
  void countJobsForArchiving_matchesFindCount() {
    persist(newPendingJob());

    Instant cutoff = Instant.now().minusSeconds(3600);
    long count = archiveStore().countJobsForArchiving(cutoff);
    var candidates = archiveStore().findJobsForArchiving(cutoff, 100);

    assertEquals(
        count, candidates.size(), "countJobsForArchiving and findJobsForArchiving should agree");
  }

  @Test
  void purgeArchivedJobs_removesOldArchives() {
    var job = completeJob(persist(newPendingJob()));
    var archived = archiveStore().archiveJob(job, "purge-test", "tck");

    int purged = archiveStore().purgeArchivedJobs(archived.getArchivedAt().plusMillis(1));

    assertTrue(purged >= 1, "purgeArchivedJobs should remove the archived record");
  }

  @Test
  void findArchivedJobs_filtersByBusinessKey() {
    var job1 = newPendingJob();
    job1.setBusinessKey("bk-archive-1");
    job1 = completeJob(persist(job1));
    archiveStore().archiveJob(job1, "test", "tck");

    var job2 = newPendingJob();
    job2.setBusinessKey("bk-archive-2");
    job2 = completeJob(persist(job2));
    archiveStore().archiveJob(job2, "test", "tck");

    var results = archiveStore().findArchivedJobs(null, "bk-archive-1", null, null, 10);

    assertEquals(1, results.size(), "findArchivedJobs should filter by businessKey");
    assertEquals("bk-archive-1", results.get(0).getBusinessKey());
  }

  @Test
  void findArchivedJobs_emptyStore_returnsEmptyList() {
    var results = archiveStore().findArchivedJobs(null, null, null, null, 10);

    assertTrue(results.isEmpty(), "findArchivedJobs on empty store should return empty list");
  }

  private JobEntity completeJob(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    return store().findById(job.getId()).orElseThrow();
  }
}
