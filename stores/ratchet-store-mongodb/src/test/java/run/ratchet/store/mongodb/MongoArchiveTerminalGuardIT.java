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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/**
 * Retention selection happens before the atomic move. If a job is reset to PENDING in that window
 * (e.g. a dashboard retry), the move must not remove the now-live job, or the retry silently
 * vanishes. The operation re-reads the batch and guards its delete on terminal status.
 */
class MongoArchiveTerminalGuardIT extends BaseDocumentStoreIT {

  @Test
  void retryDuringArchiveWindowIsNotDeleted() {
    JobEntity job = fail(store().save(newPendingJob()));

    // Stale snapshot the retention pass would carry into the archive transaction.
    JobEntity staleSnapshot = store().findById(job.getId()).orElseThrow();
    assertEquals(JobStatus.FAILED, staleSnapshot.getStatus());

    // Dashboard retry resurrects the job before the archive transaction runs.
    assertTrue(store().resetFailedToPending(job.getId()));

    // Archiving the stale FAILED snapshot must not delete the now-PENDING live job.
    assertThrows(
        RuntimeException.class,
        () -> store().archiveAndDeleteJobsBatch(List.of(staleSnapshot), "retention", "system"));

    JobEntity reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus(), "resurrected job must survive");

    List<ArchivedJobEntity> archived = store().findArchivedJobs(null, null, null, null, 10);
    assertTrue(archived.isEmpty(), "stale snapshot must not be left in the archive");
  }

  private JobEntity fail(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(job.getId(), "boom", 1);
    return store().findById(job.getId()).orElseThrow();
  }
}
