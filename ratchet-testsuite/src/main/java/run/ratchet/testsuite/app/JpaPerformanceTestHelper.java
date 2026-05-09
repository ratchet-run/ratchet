package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.UserTransaction;
import java.util.List;
import java.util.logging.Logger;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** JPA implementation using native bulk inserts. */
@ApplicationScoped
public class JpaPerformanceTestHelper implements PerformanceTestHelper {

  private static final Logger log = Logger.getLogger(JpaPerformanceTestHelper.class.getName());

  @Inject private RatchetEntityManagerProvider entityManagerProvider;

  @Inject private UserTransaction utx;

  @Override
  public void insertBackgroundRows(int count, String keyPrefix) {
    String dbType = TestRuntimeConfig.dbType();
    int chunkSize = 100_000;

    try {
      for (int offset = 0; offset < count; offset += chunkSize) {
        int batchCount = Math.min(chunkSize, count - offset);

        utx.begin();
        if ("postgresql".equals(dbType)) {
          insertPostgresqlChunk(batchCount, offset, keyPrefix);
        } else {
          insertMysqlChunk(batchCount, offset, keyPrefix);
        }
        utx.commit();

        if (count > chunkSize) {
          log.info(
              String.format("  ... inserted %d / %d background rows", offset + batchCount, count));
        }
      }

      // Refresh table statistics for accurate query planner estimates
      utx.begin();
      if ("postgresql".equals(dbType)) {
        // language=PostgreSQL
        String pgAnalyze = "ANALYZE scheduler_job";
        em().createNativeQuery(pgAnalyze).executeUpdate();
      } else {
        // language=MySQL
        String mysqlAnalyze = "ANALYZE TABLE scheduler_job";
        em().createNativeQuery(mysqlAnalyze).getResultList();
      }
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly();
      throw e;
    } catch (Exception e) {
      rollbackQuietly();
      throw new RuntimeException("Bulk insert error", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public long queryQueueWaitPercentileForClass(String targetClass, double percentile) {
    try {
      utx.begin();
      // language=SQL
      String sql =
          """
          SELECT queue_wait_ms FROM scheduler_job
          WHERE target_class = :cls AND status = 'SUCCEEDED'
            AND queue_wait_ms IS NOT NULL
          ORDER BY queue_wait_ms
          """;
      List<Number> results =
          em().createNativeQuery(sql).setParameter("cls", targetClass).getResultList();
      utx.commit();

      if (results.isEmpty()) {
        return 0;
      }
      int index = (int) Math.ceil(percentile * results.size()) - 1;
      return results.get(Math.max(0, index)).longValue();
    } catch (Exception e) {
      rollbackQuietly();
      log.warning("queue_wait_ms query error: " + e.getMessage());
      return -1;
    }
  }

  @Override
  public void assertNoFullScan(String label, Runnable storeOperation) {
    String dbType = TestRuntimeConfig.dbType();

    try {
      if ("postgresql".equals(dbType)) {
        assertNoFullScanPostgresql(label, storeOperation);
      } else {
        assertNoFullScanMysql(label, storeOperation);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Scan check error", e);
    }
  }

  private void assertNoFullScanPostgresql(String label, Runnable storeOperation) throws Exception {
    // language=PostgreSQL
    String statSql =
        """
        SELECT seq_scan, idx_scan FROM pg_stat_user_tables
        WHERE relname = 'scheduler_job'
        """;
    PostgresqlScanCounts before = readPostgresqlScanCounts(statSql);

    utx.begin();
    storeOperation.run();
    utx.commit();

    PostgresqlScanCounts after = readPostgresqlScanCounts(statSql);
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

  private PostgresqlScanCounts readPostgresqlScanCounts(String statSql) throws Exception {
    utx.begin();
    Object result = em().createNativeQuery(statSql).getSingleResult();
    utx.commit();

    if (!(result instanceof Object[] row) || row.length < 2) {
      throw new IllegalStateException("Unexpected PostgreSQL scan stats row: " + result);
    }
    return new PostgresqlScanCounts(numberAt(row, 0, "seq_scan"), numberAt(row, 1, "idx_scan"));
  }

  private long numberAt(Object[] row, int index, String columnName) {
    if (!(row[index] instanceof Number value)) {
      throw new IllegalStateException("PostgreSQL scan stat " + columnName + " was " + row[index]);
    }
    return value.longValue();
  }

  private void assertNoFullScanMysql(String label, Runnable storeOperation) throws Exception {
    // MySQL: Select_scan is session-wide (all tables), not per-table like PostgreSQL's
    // pg_stat_user_tables. Log for informational purposes only.
    utx.begin();
    storeOperation.run();
    utx.commit();

    log.info(
        String.format(
            "Scan stats [%s]: MySQL — per-table scan metrics not available, "
                + "relying on index definitions and EXPLAIN plans for verification",
            label));
  }

  private void insertPostgresqlChunk(int batchCount, int offset, String keyPrefix) {
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
    em().createNativeQuery(sql).executeUpdate();
  }

  private void insertMysqlChunk(int batchCount, int offset, String keyPrefix) {
    // job_id is BINARY(16); UUID_TO_BIN(UUID(), 1) produces a time-ordered 16-byte value.
    // language=MySQL
    String setDepthSql = "SET @@cte_max_recursion_depth = " + (batchCount + 1);
    em().createNativeQuery(setDepthSql).executeUpdate();
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_job
          (job_id, status, scheduled_time, job_type, payload, idempotency_key,
           business_key, execution_start_time, execution_end_time,
           created_at, updated_at)
        WITH RECURSIVE seq(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %d
        )
        SELECT UUID_TO_BIN(UUID(), 1),
               'SUCCEEDED', NOW() - INTERVAL 1 HOUR, 'SINGLE',
               JSON_OBJECT('target','run.ratchet.testsuite.app.TimingJob',
                           'method','execute','descriptor','()V','isStatic',true,
                           'args',JSON_ARRAY()),
               UUID(),
               CONCAT('%s-', n + %d),
               NOW() - INTERVAL 1 HOUR,
               DATE_ADD(NOW() - INTERVAL 1 HOUR, INTERVAL 10000 MICROSECOND),
               NOW(), NOW()
        FROM seq
        """
            .formatted(batchCount, keyPrefix, offset);
    em().createNativeQuery(sql).executeUpdate();
  }

  private EntityManager em() {
    return entityManagerProvider.getEntityManager();
  }

  private void rollbackQuietly() {
    try {
      utx.rollback();
    } catch (Exception ignored) {
      // best-effort rollback
    }
  }

  private record PostgresqlScanCounts(long seqScan, long idxScan) {}
}
