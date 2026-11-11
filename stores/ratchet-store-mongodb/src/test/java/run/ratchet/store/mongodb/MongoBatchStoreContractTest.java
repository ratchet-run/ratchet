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

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractBatchStoreContract;

/** MongoDB contract test for {@code BatchStore} operations. */
class MongoBatchStoreContractTest extends AbstractBatchStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @Test
  void cancelledOpenBatchIsRepairableUntilClosed() {
    var cancelled = persist(newBatchParentJob());
    persistBatch(cancelled.getId(), 4);
    var active = persist(newBatchParentJob());
    persistBatch(active.getId(), 4);
    assertTrue(store().cancelJob(cancelled.getId()));
    assertEquals(List.of(cancelled.getId()), batchStore().findCancelledOpenBatchIds(1));
    assertEquals(JobStatus.PENDING, store().getJobStatus(active.getId()));
    batchStore().closeBatch(cancelled.getId());
    assertTrue(batchStore().findCancelledOpenBatchIds(100).isEmpty());
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
}
