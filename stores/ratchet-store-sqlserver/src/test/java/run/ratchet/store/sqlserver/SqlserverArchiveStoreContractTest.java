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
package run.ratchet.store.sqlserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractArchiveStoreContract;

class SqlserverArchiveStoreContractTest extends AbstractArchiveStoreContract {

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }

  @org.junit.jupiter.api.Test
  void archiveBatchSpansParameterLimitAndDefaultBatchSize() {
    for (int size : List.of(63, 64, 1000, 2101)) {
      cleanupStore();
      var jobs = new ArrayList<JobEntity>();
      for (int i = 0; i < size; i++) {
        var job = newPendingJob();
        job.setStatus(JobStatus.SUCCEEDED);
        jobs.add(job);
      }
      store().bulkInsert(jobs);
      var ids = jobs.stream().map(JobEntity::getId).toList();
      Assertions.assertEquals(
          size, archiveStore().archiveAndDeleteJobsBatch(jobs, "parameter-boundary", "tck"));
      for (int start = 0; start < ids.size(); start += 1000) {
        Assertions.assertTrue(
            store().findByIds(ids.subList(start, Math.min(start + 1000, ids.size()))).isEmpty());
      }
      Assertions.assertEquals(
          size, archiveStore().findArchivedJobs(null, null, null, null, size).size());
    }
  }

  @org.junit.jupiter.api.Test
  void laterOuterChunkFailureRollsBackAlreadyArchivedAndDeletedPrefix() {
    var jobs = new ArrayList<JobEntity>();
    for (int i = 0; i < 1001; i++) {
      var job = newPendingJob();
      if (i < 1000) job.setStatus(JobStatus.SUCCEEDED);
      jobs.add(job);
    }
    store().bulkInsert(jobs);
    var prefixDeleted = new AtomicBoolean();
    fixture.observeNativeQueries(
        (sql, event) -> {
          if (event.completed()
              && event.operation().equals("executeUpdate")
              && sql.startsWith("DELETE FROM scheduler_job WHERE job_id IN")) {
            Assertions.assertEquals(1000, ((Number) event.result()).intValue());
            prefixDeleted.set(true);
          }
        });
    try {
      Assertions.assertThrows(
          RuntimeException.class,
          () -> archiveStore().archiveAndDeleteJobsBatch(jobs, "later-chunk-failure", "tck"));
    } finally {
      fixture.observeNativeQueries((sql, event) -> {});
    }
    Assertions.assertTrue(
        prefixDeleted.get(), "first outer chunk must actually delete before injected failure");
    Assertions.assertEquals(
        1000,
        store().findByIds(jobs.subList(0, 1000).stream().map(JobEntity::getId).toList()).size());
    Assertions.assertEquals(
        JobStatus.PENDING, store().findById(jobs.get(1000).getId()).orElseThrow().getStatus());
    Assertions.assertTrue(archiveStore().findArchivedJobs(null, null, null, null, 1100).isEmpty());
  }
}
