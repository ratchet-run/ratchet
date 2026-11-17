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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.ResourcePermitStore;

/** Verifies stored column values, without relying on collation-sensitive equality predicates. */
class SqlserverUnicodeColumnTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @BeforeEach
  @AfterEach
  void cleanup() {
    fixture.cleanupStore();
  }

  @Test
  void callerKeysAreStoredWithoutCodePageLoss() throws Exception {
    var store = fixture.store();
    var job = fixture.newPendingJob();
    job.setBusinessKey("business-ā中");
    job.setIdempotencyKey("idem-ā中");
    job.setResourceName("resource-ā中");
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey("sig-ā中");
    job.setSignalTimeout(Instant.now().plusSeconds(600));
    store.create(job);
    store.insertTags(job.getId(), List.of("tag-ā中"));
    store.upsertHeartbeat("node-ā中", Instant.now());
    store
        .capability(ResourcePermitStore.class)
        .orElseThrow()
        .configureResource("resource-ā中", 1, 1000, "description-ā中");
    assertTrue(
        store
            .capability(LockStore.class)
            .orElseThrow()
            .tryLock("lock-ā中", Duration.ofMinutes(5), "node-ā中"));

    try (var connection = fixture.openConnection();
        var statement = connection.createStatement()) {
      for (String[] expected :
          List.of(
              new String[] {"scheduler_job", "business_key", "business-ā中"},
              new String[] {"scheduler_job_queue", "business_key", "business-ā中"},
              new String[] {"scheduler_job", "idempotency_key", "idem-ā中"},
              new String[] {"scheduler_idempotency_key", "idempotency_key", "idem-ā中"},
              new String[] {"scheduler_job", "resource_name", "resource-ā中"},
              new String[] {"scheduler_resource_limit", "resource_name", "resource-ā中"},
              new String[] {"scheduler_job_queue", "signal_key", "sig-ā中"},
              new String[] {"scheduler_job_tag", "tag", "tag-ā中"},
              new String[] {"scheduler_node", "node_id", "node-ā中"},
              new String[] {"scheduler_lock", "lock_name", "lock-ā中"},
              new String[] {"scheduler_lock", "owner_node", "node-ā中"})) {
        try (var rows = statement.executeQuery("SELECT " + expected[1] + " FROM " + expected[0])) {
          assertTrue(rows.next(), expected[0]);
          assertEquals(expected[2], rows.getString(1), expected[0] + "." + expected[1]);
        }
      }
    }
  }
}
