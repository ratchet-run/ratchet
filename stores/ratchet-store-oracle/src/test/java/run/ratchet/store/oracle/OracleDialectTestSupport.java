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
package run.ratchet.store.oracle;

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.Locale;
import java.util.UUID;
import java.util.logging.Logger;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * Oracle {@link SqlDialectTestSupport}: RAW(16) ids, no foreign-key toggling, and row-level DELETE.
 */
public final class OracleDialectTestSupport implements SqlDialectTestSupport {

  private static final Logger log = Logger.getLogger(OracleDialectTestSupport.class.getName());

  /** Public no-arg constructor required by {@link java.util.ServiceLoader}. */
  public OracleDialectTestSupport() {}

  @Override
  public void disableForeignKeyChecks(EntityManager em) {
    // Oracle has no session-level foreign-key toggle; DELETE respects the child-before-parent
    // ordering the caller iterates in.
  }

  @Override
  public void enableForeignKeyChecks(EntityManager em) {
    // No-op — see disableForeignKeyChecks.
  }

  @Override
  public void clearTable(EntityManager em, String table) {
    // A concurrent TRUNCATE both fails outright on tables that enabled foreign keys reference
    // (ORA-02266) and resets a table's data-object number, so any in-flight poller query against it
    // dies with ORA-08103. Row-level DELETE is MVCC-friendly and coexists with the live poller.
    em.createNativeQuery("DELETE FROM " + table).executeUpdate();
  }

  @Override
  public Object jobIdParam(UUID jobId) {
    return SqlDialectTestSupport.uuidToBigEndianBytes(jobId);
  }

  @Override
  public void insertTerminalChunk(EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Cold archive rows: terminal_status='SUCCEEDED', no queue row. CONNECT BY LEVEL generates the
    // chunk; SYS_GUID() is a fresh RAW(16) per row (random is fine — never correlated to a queue
    // row). The payload literal is implicitly stored into the CLOB column.
    // language=Oracle
    String sql =
        """
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key,
           created_at, terminal_status, terminated_at,
           execution_start_time, execution_end_time, queue_wait_ms)
        SELECT SYS_GUID(), 'SINGLE',
               '{"target":"run.ratchet.testsuite.app.TimingJob",\
        "method":"execute","descriptor":"()V",\
        "isStatic":true,"args":[]}',
               RAWTOHEX(SYS_GUID()),
               '%2$s-' || (LEVEL + %1$d),
               SYSTIMESTAMP, 'SUCCEEDED', SYSTIMESTAMP - INTERVAL '1' HOUR,
               SYSTIMESTAMP - INTERVAL '1' HOUR,
               SYSTIMESTAMP - INTERVAL '1' HOUR + INTERVAL '0.01' SECOND,
               10
        FROM dual CONNECT BY LEVEL <= %3$d
        """
            .formatted(offset, keyPrefix, batchCount);
    em.createNativeQuery(sql).executeUpdate();
  }

  @Override
  public void insertPendingQueueChunk(
      EntityManager em, int batchCount, int offset, String keyPrefix) {
    // Live PENDING jobs: a cold parent plus a far-future hot queue row. Deterministic RAW(16) ids —
    // HEXTORAW(LPAD(TO_CHAR(n, 'FMXXXX...'), 32, '0')) — let both inserts agree on job_id without
    // an
    // anti-join.
    // language=Oracle
    String coldSql =
        """
        INSERT INTO scheduler_job
          (job_id, job_type, payload, idempotency_key, business_key, created_at)
        SELECT HEXTORAW(LPAD(TO_CHAR(LEVEL + %1$d, 'FMXXXXXXXXXXXXXXXX'), 32, '0')), 'SINGLE',
               '{"target":"run.ratchet.testsuite.app.TimingJob",\
        "method":"execute","descriptor":"()V",\
        "isStatic":true,"args":[]}',
               '%2$s-key-' || (LEVEL + %1$d),
               '%2$s-' || (LEVEL + %1$d),
               SYSTIMESTAMP
        FROM dual CONNECT BY LEVEL <= %3$d
        """
            .formatted(offset, keyPrefix, batchCount);
    em.createNativeQuery(coldSql).executeUpdate();

    // language=Oracle
    String hotSql =
        """
        INSERT INTO scheduler_job_queue
          (job_id, status, job_type, scheduled_time, updated_at)
        SELECT HEXTORAW(LPAD(TO_CHAR(LEVEL + %1$d, 'FMXXXXXXXXXXXXXXXX'), 32, '0')),
               'PENDING', 'SINGLE', SYSTIMESTAMP + INTERVAL '365' DAY(3), SYSTIMESTAMP
        FROM dual CONNECT BY LEVEL <= %2$d
        """
            .formatted(offset, batchCount);
    em.createNativeQuery(hotSql).executeUpdate();
  }

  @Override
  public void analyzeTable(EntityManager em, String table) {
    // Oracle gathers optimizer statistics via DBMS_STATS; the table name is upper-cased to match
    // the data-dictionary identifier. Runs in its own committed transaction (the caller commits the
    // insert chunks first), so it does not deadlock against uncommitted DML.
    String block =
        "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '" + table.toUpperCase(Locale.ROOT) + "'); END;";
    em.createNativeQuery(block).executeUpdate();
  }

  @Override
  public void assertNoFullScan(
      EntityManager em, UserTransaction utx, String table, String label, Runnable storeOperation)
      throws Exception {
    // Oracle has no cheap per-table sequential-scan counter (v$ views are instance-wide and require
    // privileges Testcontainers does not grant), so the claim path's index usage is verified by the
    // schema-conformance TCK and EXPLAIN plans instead. Log for informational purposes only.
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
            "Scan stats [%s] on %s: Oracle — per-table scan metrics not available, "
                + "relying on index definitions and EXPLAIN plans for verification",
            label, table));
  }
}
