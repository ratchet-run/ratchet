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

import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractSignalStoreContract;

class MongoSignalStoreContractTest extends AbstractSignalStoreContract {

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
  void deliverSignalByIdSetsUpdatedAtToDeliveredAt() {
    JobEntity job = newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey("mongo-delivered-at");
    job.setSignalTimeout(Instant.parse("2026-05-05T13:00:00Z"));
    JobEntity saved = persist(job);
    Instant deliveredAt = Instant.parse("2026-05-05T12:00:00Z");

    int delivered =
        store()
            .deliverSignalById(
                saved.getId(),
                null,
                null,
                "APPROVED",
                null,
                "admin",
                deliveredAt,
                "mongo-delivery");

    assertEquals(1, delivered);
    assertEquals(deliveredAt, store().findById(saved.getId()).orElseThrow().getUpdatedAt());
  }
}
