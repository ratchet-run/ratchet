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

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractActiveBusinessKeyContract;

class MongoActiveBusinessKeyContractTest extends AbstractActiveBusinessKeyContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @Test
  void archiveJob_heldJob_throwsAndChangesNothing() {
    // Mongo archiving preserves a held active job and its reservation.
    var archives = archiveStore();
    String bk = "abk-" + UUID.randomUUID();
    var job = persist(keyedJob(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    var canceled = store().findById(job.getId()).orElseThrow();
    assertThrows(
        RatchetTransientStoreException.class, () -> archives.archiveJob(canceled, "test", "tck"));
    assertEquals(JobStatus.CANCELED, store().findById(job.getId()).orElseThrow().getStatus());
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(bk).orElseThrow());
    assertTrue(archives.findArchivedJob(job.getId()).isEmpty());
  }

  @Test
  void archiveJob_afterRelease_freesKey() {
    // Mongo archiving after release allows the key to be reused.
    var archives = archiveStore();
    String bk = "abk-" + UUID.randomUUID();
    var job = persist(keyedJob(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertTrue(store().releaseBusinessKeyHold(job.getId(), claim.getClaimSeq()));
    archives.archiveJob(store().findById(job.getId()).orElseThrow(), "test", "tck");
    assertTrue(store().findById(job.getId()).isEmpty());
    persist(keyedJob(bk));
  }

  private JobEntity keyedJob(String key) {
    var job = newPendingJob();
    job.setBusinessKey(key);
    return job;
  }

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @Override
  protected void insertReservationWithoutOwner(
      String businessKey, UUID ownerId, String ownerTable) {
    fixture
        .database()
        .getCollection("scheduler_business_key_reservation")
        .insertOne(
            new Document("_id", businessKey)
                .append("owner_job_id", ownerId)
                .append("owner_table", ownerTable)
                .append("reserved_at", new Date()));
  }

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
}
