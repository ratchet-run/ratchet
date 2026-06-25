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
package run.ratchet.store.postgresql;

import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.UUID;
import java.util.logging.Logger;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * PostgreSQL {@link SqlDialectTestSupport}: native {@code uuid} ids, no foreign-key toggling, and
 * row-level DELETE. PostgreSQL is the only dialect that can attribute a sequential scan to a
 * specific table via {@code pg_stat_user_tables}, so it carries the only enforcing scan check.
 */
public final class PostgresqlDialectTestSupport implements SqlDialectTestSupport {

  private static final Logger log = Logger.getLogger(PostgresqlDialectTestSupport.class.getName());

  /** Public no-arg constructor required by {@link java.util.ServiceLoader}. */
  public PostgresqlDialectTestSupport() {}

  @Override
  public void disableForeignKeyChecks(EntityManager em) {
    // PostgreSQL has no session-level foreign-key toggle; DELETE respects the child-before-parent
    // ordering the caller iterates in.
  }

  @Override
  public void enableForeignKeyChecks(EntityManager em) {
    // No-op — see disableForeignKeyChecks.
  }

  @Override
  public void clearTable(EntityManager em, String table) {
    em.createNativeQuery("DELETE FROM " + table).executeUpdate();
  }

  @Override
  public Object jobIdParam(UUID jobId) {
    // PostgreSQL's native uuid column type accepts the UUID directly.
    return jobId;
  }

  @Override
  public void insertBackgroundChunk(
      EntityManager em, int batchCount, int offset, String keyPrefix) {
    // job_id is native uuid; gen_random_uuid() returns a fresh v4 per row.
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_job
          (job_id, status, scheduled_time, job_type, payload, idempotency_key,
           business_key, execution_start_time, execution_end_time,
           created_at, updated_at)
        SELECT gen_random_uuid(),
               'SUCCEEDED', NOW() - INTERVAL '1 hour', 'SINGLE',
               '{"target":"run.ratchet.testsuite.app.TimingJob",\
        "method":"execute","descriptor":"()V",\
        "isStatic":true,"args":[]}'::jsonb,
               gen_random_uuid()::text,
               '%s-' || (g + %d),
               NOW() - INTERVAL '1 hour',
               NOW() - INTERVAL '1 hour' + INTERVAL '10 milliseconds',
               NOW(), NOW()
        FROM generate_series(1, %d) AS g
        """
            .formatted(keyPrefix, offset, batchCount);
    em.createNativeQuery(sql).executeUpdate();
  }

  @Override
  public void analyzeSchedulerJob(EntityManager em) {
    // language=PostgreSQL
    String pgAnalyze = "ANALYZE scheduler_job";
    em.createNativeQuery(pgAnalyze).executeUpdate();
  }

  @Override
  public void assertNoFullScan(
      EntityManager em, UserTransaction utx, String label, Runnable storeOperation)
      throws Exception {
    // language=PostgreSQL
    String statSql =
        """
        SELECT seq_scan, idx_scan FROM pg_stat_user_tables
        WHERE relname = 'scheduler_job'
        """;
    ScanCounts before = readScanCounts(em, utx, statSql);

    utx.begin();
    try {
      storeOperation.run();
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly(utx);
      throw e;
    }

    ScanCounts after = readScanCounts(em, utx, statSql);
    long seqDelta = after.seqScan() - before.seqScan();
    long idxDelta = after.idxScan() - before.idxScan();

    log.info(
        String.format(
            "Scan stats [%s]: seq_scan delta=%d, idx_scan delta=%d", label, seqDelta, idxDelta));

    if (seqDelta != 0) {
      throw new AssertionError(
          label + ": sequential scan detected on scheduler_job (seq_scan delta=" + seqDelta + ")");
    }
  }

  private ScanCounts readScanCounts(EntityManager em, UserTransaction utx, String statSql)
      throws Exception {
    utx.begin();
    Object result;
    try {
      result = em.createNativeQuery(statSql).getSingleResult();
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly(utx);
      throw e;
    }

    if (!(result instanceof Object[] row) || row.length < 2) {
      throw new IllegalStateException("Unexpected PostgreSQL scan stats row: " + result);
    }
    return new ScanCounts(numberAt(row, 0, "seq_scan"), numberAt(row, 1, "idx_scan"));
  }

  private long numberAt(Object[] row, int index, String columnName) {
    if (!(row[index] instanceof Number value)) {
      throw new IllegalStateException("PostgreSQL scan stat " + columnName + " was " + row[index]);
    }
    return value.longValue();
  }

  private record ScanCounts(long seqScan, long idxScan) {}
}
