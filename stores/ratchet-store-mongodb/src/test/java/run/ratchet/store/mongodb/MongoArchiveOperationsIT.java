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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

class MongoArchiveOperationsIT extends BaseDocumentStoreIT {

  @Test
  void archiveJobsBatchMovesSourceJobsInsideMongoTransaction() {
    JobEntity first = complete(store().save(newPendingJob()));
    JobEntity second = complete(store().save(newPendingJob()));

    int archived = store().archiveJobsBatch(List.of(first, second), "retention", "system");

    assertEquals(2, archived);
    assertTrue(store().findById(first.getId()).isEmpty());
    assertTrue(store().findById(second.getId()).isEmpty());

    Set<UUID> archivedOriginalIds =
        store().findArchivedJobs(null, null, null, null, 10).stream()
            .map(ArchivedJobEntity::getOriginalJobId)
            .collect(Collectors.toSet());
    assertEquals(Set.of(first.getId(), second.getId()), archivedOriginalIds);
  }

  private JobEntity complete(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    return store().findById(job.getId()).orElseThrow();
  }
}
