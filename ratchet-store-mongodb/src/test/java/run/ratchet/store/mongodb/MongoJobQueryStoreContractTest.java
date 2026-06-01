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

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobQueryStoreContract;

/** MongoDB contract test for {@code JobQueryStore} operations. */
class MongoJobQueryStoreContractTest extends AbstractJobQueryStoreContract {

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
  void malformedCursorFallsBackToOffsetPagination() {
    for (int i = 0; i < 4; i++) {
      persist(newPendingJob());
    }

    List<UUID> expectedIds =
        store().searchJobs(JobFilter.builder().build(), 2, 1).stream()
            .map(JobEntity::getId)
            .toList();
    List<UUID> actualIds =
        store().searchJobs(JobFilter.builder().cursor("not-base64").build(), 2, 1).stream()
            .map(JobEntity::getId)
            .toList();

    assertEquals(expectedIds, actualIds, "Malformed cursors should degrade to offset pagination");
  }
}
