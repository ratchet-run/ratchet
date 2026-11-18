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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverJobClaimOperationsTest {

  @Test
  void claimSelectProjectsDependsOnFromColdMetadata() {
    assertEquals(expectedClaimSelectClause(), SqlserverJobClaimOperations.claimSelectClause());
  }

  @Test
  void markingLockedRowsRunningRejectsAnUpdateCountMismatch() {
    EntityManager em = mock(EntityManager.class);
    Query select = mock(Query.class);
    Query update = mock(Query.class);
    when(em.createNativeQuery(anyString())).thenReturn(select, update);
    Object[] row = {
      UuidByteArrayConverter.toBytes(UUID.randomUUID()),
      "PENDING",
      "SINGLE",
      2,
      Timestamp.from(Instant.now().minusSeconds(60)),
      0,
      60,
      null,
      null,
      null,
      0,
      3,
      null,
      null,
      0L
    };
    when(select.getResultList()).thenReturn(Collections.singletonList(row));
    when(update.executeUpdate()).thenReturn(0);
    var operations = new SqlserverJobClaimOperations(new SqlserverStoreContext(em, 0), null);
    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                operations.claimNextBatchOptimized(
                    JobExecutionType.SINGLE,
                    1,
                    "node",
                    NodeTagFilter.NONE,
                    ExecutionTargetFilter.any()));
    assertEquals("Expected to mark 1 locked claims RUNNING, updated 0", failure.getMessage());
  }

  private static String expectedClaimSelectClause() {
    return "job_id, status, job_type, priority, scheduled_time, version, timeout_sec, picked_by,"
        + " picked_at, business_key, attempts, max_retries, execution_target,"
        + " (SELECT cold_job.depends_on FROM scheduler_job cold_job"
        + " WHERE cold_job.job_id = scheduler_job_queue.job_id) AS depends_on, claim_seq";
  }
}
