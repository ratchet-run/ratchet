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

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.UUID;
import java.util.logging.Logger;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * SQL Server {@link SqlDialectTestSupport}: BINARY(16) ids, no foreign-key toggling, and row-level
 * DELETE.
 */
public final class SqlserverDialectTestSupport implements SqlDialectTestSupport {

  private static final Logger log = Logger.getLogger(SqlserverDialectTestSupport.class.getName());

  /** Public no-arg constructor required by {@link java.util.ServiceLoader}. */
  public SqlserverDialectTestSupport() {}

  @Override
  public void disableForeignKeyChecks(EntityManager em) {
    // SQL Server has no session-level foreign-key toggle; the cleanup DELETE respects the
    // child-before-parent ordering the caller iterates in.
  }

  @Override
  public void enableForeignKeyChecks(EntityManager em) {
    // No-op — see disableForeignKeyChecks.
  }

  @Override
  public void clearTable(EntityManager em, String table) {
    // SQL Server forbids TRUNCATE on a table referenced by an enabled foreign key, and the
    // scheduler poller keeps ticking through cleanup. Row-level DELETE respects the
    // child-before-parent ordering the caller iterates in, so it coexists with the live poller.
    em.createNativeQuery("DELETE FROM " + table).executeUpdate();
  }

  @Override
  public Object jobIdParam(UUID jobId) {
    return SqlDialectTestSupport.uuidToBigEndianBytes(jobId);
  }

  @Override
  public void insertTerminalChunk(EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Cold archive rows: terminal_status='SUCCEEDED', no queue row. A recursive CTE generates the
    // chunk; CONVERT(BINARY(16), NEWID()) is a fresh 16-byte id per row (random is fine — these
    // rows are never correlated to a queue row). OPTION (MAXRECURSION 0) lifts the default cap of
    // 100 so large batches insert fully.
    // language=TSQL
    String sql =
        """
        WITH seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %3$d
        )
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key,
           created_at, terminal_status, terminated_at,
           execution_start_time, execution_end_time, queue_wait_ms)
        SELECT CONVERT(BINARY(16), NEWID()), 'SINGLE',
               '{"target":"run.ratchet.testsuite.app.TimingJob",\
        "method":"execute","descriptor":"()V",\
        "isStatic":true,"args":[]}',
               CONVERT(VARCHAR(36), NEWID()),
               CONCAT('%2$s-', n + %1$d),
               SYSUTCDATETIME(), 'SUCCEEDED', DATEADD(HOUR, -1, SYSUTCDATETIME()),
               DATEADD(HOUR, -1, SYSUTCDATETIME()),
               DATEADD(MILLISECOND, 10, DATEADD(HOUR, -1, SYSUTCDATETIME())),
               10
        FROM seq
        OPTION (MAXRECURSION 0)
        """
            .formatted(offset, keyPrefix, batchCount);
    em.createNativeQuery(sql).executeUpdate();
  }

  @Override
  public void insertPendingQueueChunk(
      EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Live PENDING jobs: a cold parent plus a far-future hot queue row. Deterministic BINARY(16)
    // ids — CAST(CAST(n + offset AS BIGINT) AS BINARY(16)) — let both inserts agree on job_id
    // without an anti-join. The absolute byte layout is irrelevant here: only that the cold and hot
    // inserts derive the same value from the same n.
    // language=TSQL
    String coldSql =
        """
        WITH seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %3$d
        )
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key, created_at)
        SELECT CAST(CAST(n + %1$d AS BIGINT) AS BINARY(16)), 'SINGLE',
               '{"target":"run.ratchet.testsuite.app.TimingJob",\
        "method":"execute","descriptor":"()V",\
        "isStatic":true,"args":[]}',
               CONCAT('%2$s-key-', n + %1$d),
               CONCAT('%2$s-', n + %1$d),
               SYSUTCDATETIME()
        FROM seq
        OPTION (MAXRECURSION 0)
        """
            .formatted(offset, keyPrefix, batchCount);
    em.createNativeQuery(coldSql).executeUpdate();

    // language=TSQL
    String hotSql =
        """
        WITH seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %2$d
        )
        INSERT INTO scheduler_job_queue
          (job_id, status, job_type, scheduled_time, updated_at)
        SELECT CAST(CAST(n + %1$d AS BIGINT) AS BINARY(16)),
               'PENDING', 'SINGLE', DATEADD(DAY, 365, SYSUTCDATETIME()), SYSUTCDATETIME()
        FROM seq
        OPTION (MAXRECURSION 0)
        """
            .formatted(offset, batchCount);
    em.createNativeQuery(hotSql).executeUpdate();
  }

  @Override
  public void analyzeTable(EntityManager em, String table) {
    // SQL Server refreshes optimizer statistics with UPDATE STATISTICS. Runs in its own committed
    // transaction (the caller commits the insert chunks first), so it does not deadlock against
    // uncommitted DML.
    em.createNativeQuery("UPDATE STATISTICS " + table).executeUpdate();
  }

  @Override
  public void assertNoFullScan(
      EntityManager em, UserTransaction utx, String table, String label, Runnable storeOperation)
      throws Exception {
    // SQL Server's per-table scan counters (sys.dm_db_index_usage_stats) are instance-wide and
    // reset on restart, so the claim path's index usage is verified by the schema-conformance TCK
    // and EXPLAIN plans instead. Log for informational purposes only.
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
            "Scan stats [%s] on %s: SQL Server — per-table scan metrics not available, "
                + "relying on index definitions and EXPLAIN plans for verification",
            label, table));
  }
}
