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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobBulkStoreContract;

/** MongoDB contract test for {@code JobBulkStore} operations. */
class MongoJobBulkStoreContractTest extends AbstractJobBulkStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

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

  @Test
  void missingCrashCountIsZeroEvenWithZeroBudget() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    fixture
        .database()
        .getCollection("scheduler_job")
        .updateOne(eq("_id", job.getId()), unset("crash_count"));

    var recovery = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 0, 100);

    assertEquals(0, recovery.reset());
    assertEquals(1, recovery.exhausted().size());
    assertEquals(0, recovery.exhausted().get(0).crashCount());
  }

  @Test
  void savePreservesStoreOwnedCrashBudget() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    assertEquals(1, store().resetOrphanJobsForNode("dead", 3, 100).reset());
    JobEntity pending = store().findById(job.getId()).orElseThrow();
    store().save(pending);

    Document stored =
        fixture.database().getCollection("scheduler_job").find(eq("_id", job.getId())).first();
    assertEquals(1, stored.getInteger("crash_count"));
  }
}
