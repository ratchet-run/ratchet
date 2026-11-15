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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.persistence.Persistence;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;

/** Verifies INSERT IGNORE counts under both MySQL Connector/J affected-row settings. */
class MysqlIdempotencyKeyReserveAffectedRowsTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tryReserveReportsOnlyNewReservations(boolean useAffectedRows) {
    var fixture = new MysqlTestFixture();
    var container = fixture.container();
    var props = new HashMap<String, Object>(fixture.jpaProperties());
    props.put(
        "jakarta.persistence.jdbc.url",
        container.getJdbcUrl() + "&useAffectedRows=" + useAffectedRows);
    props.put("jakarta.persistence.jdbc.user", container.getUsername());
    props.put("jakarta.persistence.jdbc.password", container.getPassword());
    var emf = Persistence.createEntityManagerFactory("ratchet-mysql-tck", props);
    try {
      var em = emf.createEntityManager();
      var tx = em.getTransaction();
      try {
        tx.begin();
        var writes =
            new MysqlJobWriteOperations(
                new MysqlStoreContext(em, mock(MetricsCollector.class)), null, null);
        Timestamp now = Timestamp.from(Instant.now());
        JobEntity job = fixture.newPendingJob();
        job.setId(UUID.randomUUID());
        job.setIdempotencyKey(UUID.randomUUID().toString());
        assertTrue(writes.tryReserveIdempotencyKey(job, now));
        assertFalse(writes.tryReserveIdempotencyKey(job, now));
        job.setIdempotencyKey(UUID.randomUUID().toString());
        assertTrue(writes.tryReserveIdempotencyKey(job, now));
      } finally {
        // Every reservation belongs to this transaction; rollback also cleans up on assertion
        // failure.
        if (tx.isActive()) {
          tx.rollback();
        }
        em.close();
      }
    } finally {
      emf.close();
    }
  }
}
