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
import static com.mongodb.client.model.Updates.unset;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.mongodb.MongoFieldNames.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;

/** MongoDB contract test for {@code JobCrudStore} operations. */
class MongoJobCrudStoreContractTest extends AbstractJobCrudStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @Test
  void revivalViaStatusUpdateClearsCancelClaim() {
    JobEntity claim = runningCancel();
    store().updateJobStatus(claim.getId(), JobStatus.PENDING, null);
    assertFalse(store().wasCanceledUnderClaim(claim.getId(), claim.getClaimSeq()));
    assertFalse(
        fixture
            .database()
            .getCollection("scheduler_job")
            .find(eq(ID, claim.getId()))
            .first()
            .containsKey(CANCELED_CLAIM_SEQ));
  }

  @Test
  void revivalViaSaveClearsCancelClaim() {
    JobEntity claim = runningCancel();
    JobEntity canceled = store().findById(claim.getId()).orElseThrow();
    canceled.setStatus(JobStatus.PENDING);
    store().save(canceled);
    assertFalse(store().wasCanceledUnderClaim(claim.getId(), claim.getClaimSeq()));
    assertFalse(
        fixture
            .database()
            .getCollection("scheduler_job")
            .find(eq(ID, claim.getId()))
            .first()
            .containsKey(CANCELED_CLAIM_SEQ));
  }

  @Test
  void legacyCancelWithNoRecordedClaimIsNotAttributed() {
    JobEntity claim = runningCancel();
    fixture
        .database()
        .getCollection("scheduler_job")
        .updateOne(eq(ID, claim.getId()), unset(CANCELED_CLAIM_SEQ));
    assertFalse(store().wasCanceledUnderClaim(claim.getId(), claim.getClaimSeq()));
  }

  private JobEntity runningCancel() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "mongo-attribution"));
    JobEntity claim = store().findById(job.getId()).orElseThrow();
    assertTrue(store().cancelJob(job.getId()));
    assertTrue(store().wasCanceledUnderClaim(job.getId(), claim.getClaimSeq()));
    return claim;
  }

  @AfterAll
  static void closeFixture() {
    fixture.close();
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

  @Override
  public boolean supportsTransactionalRollback() {
    return fixture.supportsTransactionalRollback();
  }

  @Override
  public boolean isStaleWriteException(Throwable t) {
    return fixture.isStaleWriteException(t);
  }

  @Override
  public int countStoreStatements(Runnable work) {
    return fixture.countStoreStatements(work);
  }
}
