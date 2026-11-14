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

import java.util.Date;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractActiveBusinessKeyContract;

class MongoActiveBusinessKeyContractTest extends AbstractActiveBusinessKeyContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

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
