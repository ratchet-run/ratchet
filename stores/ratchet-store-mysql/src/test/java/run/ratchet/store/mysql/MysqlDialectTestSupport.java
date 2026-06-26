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

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.UUID;
import java.util.logging.Logger;
import run.ratchet.tck.store.SqlDialectTestSupport;

/** MySQL {@link SqlDialectTestSupport}: BINARY(16) ids, foreign-key toggling, and TRUNCATE. */
public final class MysqlDialectTestSupport implements SqlDialectTestSupport {

  private static final Logger log = Logger.getLogger(MysqlDialectTestSupport.class.getName());

  /** Public no-arg constructor required by {@link java.util.ServiceLoader}. */
  public MysqlDialectTestSupport() {}

  @Override
  public void disableForeignKeyChecks(EntityManager em) {
    em.createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
  }

  @Override
  public void enableForeignKeyChecks(EntityManager em) {
    em.createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
  }

  @Override
  public void clearTable(EntityManager em, String table) {
    em.createNativeQuery("TRUNCATE TABLE " + table).executeUpdate();
  }

  @Override
  public Object jobIdParam(UUID jobId) {
    return SqlDialectTestSupport.uuidToBigEndianBytes(jobId);
  }

  @Override
  public void insertTerminalChunk(EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Cold archive rows: terminal_status='SUCCEEDED', no queue row. job_id is BINARY(16);
    // UUID_TO_BIN(UUID(), 1) produces a time-ordered 16-byte value (random is fine here — these
    // rows are never correlated to a queue row).
    setRecursionDepth(em, batchCount);
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key,
           created_at, terminal_status, terminated_at,
           execution_start_time, execution_end_time, queue_wait_ms)
        WITH RECURSIVE seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %d
        )
        SELECT UUID_TO_BIN(UUID(), 1), 'SINGLE',
               JSON_OBJECT('target','run.ratchet.testsuite.app.TimingJob',
                           'method','execute','descriptor','()V','isStatic',true,
                           'args',JSON_ARRAY()),
               UUID(),
               CONCAT('%s-', n + %d),
               NOW(), 'SUCCEEDED', NOW() - INTERVAL 1 HOUR,
               NOW() - INTERVAL 1 HOUR,
               DATE_ADD(NOW() - INTERVAL 1 HOUR, INTERVAL 10000 MICROSECOND),
               10
        FROM seq
        """
            .formatted(batchCount, keyPrefix, offset);
    em.createNativeQuery(sql).executeUpdate();
  }

  @Override
  public void insertPendingQueueChunk(
      EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Live PENDING jobs: a cold parent plus a far-future hot queue row. Deterministic BINARY(16)
    // ids — UNHEX(LPAD(HEX(n), 32, '0')) — let both inserts agree on job_id without an anti-join.
    setRecursionDepth(em, batchCount);
    // language=MySQL
    String coldSql =
        """
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key, created_at)
        WITH RECURSIVE seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %1$d
        )
        SELECT UNHEX(LPAD(HEX(n + %2$d), 32, '0')), 'SINGLE',
               JSON_OBJECT('target','run.ratchet.testsuite.app.TimingJob',
                           'method','execute','descriptor','()V','isStatic',true,
                           'args',JSON_ARRAY()),
               CONCAT('%3$s-key-', n + %2$d),
               CONCAT('%3$s-', n + %2$d),
               NOW()
        FROM seq
        """
            .formatted(batchCount, offset, keyPrefix);
    em.createNativeQuery(coldSql).executeUpdate();

    // language=MySQL
    String hotSql =
        """
        INSERT INTO scheduler_job_queue
          (job_id, status, job_type, scheduled_time, updated_at)
        WITH RECURSIVE seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %1$d
        )
        SELECT UNHEX(LPAD(HEX(n + %2$d), 32, '0')), 'PENDING', 'SINGLE',
               NOW() + INTERVAL 365 DAY, NOW()
        FROM seq
        """
            .formatted(batchCount, offset);
    em.createNativeQuery(hotSql).executeUpdate();
  }

  private void setRecursionDepth(EntityManager em, int batchCount) {
    // language=MySQL
    String setDepthSql = "SET @@cte_max_recursion_depth = " + (batchCount + 1);
    em.createNativeQuery(setDepthSql).executeUpdate();
  }

  @Override
  public void analyzeTable(EntityManager em, String table) {
    // language=MySQL
    String mysqlAnalyze = "ANALYZE TABLE " + table;
    em.createNativeQuery(mysqlAnalyze).getResultList();
  }

  @Override
  public void assertNoFullScan(
      EntityManager em, UserTransaction utx, String table, String label, Runnable storeOperation)
      throws Exception {
    // MySQL: Select_scan is session-wide (all tables), not per-table like PostgreSQL's
    // pg_stat_user_tables. Log for informational purposes only.
    utx.begin();
    try {
      storeOperation.run();
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly(utx);
      throw e;
    }

    log.info(
        String.format(
            "Scan stats [%s] on %s: MySQL — per-table scan metrics not available, "
                + "relying on index definitions and EXPLAIN plans for verification",
            label, table));
  }
}
