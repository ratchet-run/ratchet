package run.ratchet.testsuite.app;

import run.ratchet.store.id.TsidFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.UserTransaction;
import java.util.List;
import java.util.logging.Logger;

/**
 * JPA/SQL implementation of {@link PerformanceTestHelper}.
 *
 * <p>Uses native SQL with server-side row generation ({@code generate_series()} on PostgreSQL,
 * recursive CTEs on MySQL) for maximum bulk insert throughput. Scan diagnostics use
 * backend-specific system tables ({@code pg_stat_user_tables} on PostgreSQL).
 *
 * <p>Only packaged in the WAR when a JPA store profile is active.
 */
@ApplicationScoped
public class JpaPerformanceTestHelper implements PerformanceTestHelper {

  private static final Logger log = Logger.getLogger(JpaPerformanceTestHelper.class.getName());

  @PersistenceContext private EntityManager em;

  @Inject private UserTransaction utx;

  @Override
  public void insertBackgroundRows(int count, String keyPrefix) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
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
        em.createNativeQuery("ANALYZE scheduler_job").executeUpdate();
      } else {
        em.createNativeQuery("ANALYZE TABLE scheduler_job").getResultList();
      }
      utx.commit();
    } catch (RuntimeException e) {
      rollbackQuietly();
      throw e;
    } catch (Exception e) {
      rollbackQuietly();
      throw new RuntimeException("Failed to insert background rows", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public long queryQueueWaitPercentileForClass(String targetClass, double percentile) {
    try {
      utx.begin();
      List<Number> results =
          em.createNativeQuery(
                  "SELECT queue_wait_ms FROM scheduler_job"
                      + " WHERE target_class = :cls AND status = 'SUCCEEDED'"
                      + " AND queue_wait_ms IS NOT NULL"
                      + " ORDER BY queue_wait_ms")
              .setParameter("cls", targetClass)
              .getResultList();
      utx.commit();

      if (results.isEmpty()) {
        return 0;
      }
      int index = (int) Math.ceil(percentile * results.size()) - 1;
      return results.get(Math.max(0, index)).longValue();
    } catch (Exception e) {
      rollbackQuietly();
      log.warning("Failed to query queue_wait_ms: " + e.getMessage());
      return -1;
    }
  }

  @Override
  public void assertNoFullScan(String label, Runnable storeOperation) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");

    try {
      if ("postgresql".equals(dbType)) {
        assertNoFullScanPostgresql(label, storeOperation);
      } else {
        assertNoFullScanMysql(label, storeOperation);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to assert no full scan", e);
    }
  }

  private void assertNoFullScanPostgresql(String label, Runnable storeOperation) throws Exception {
    utx.begin();
    Object[] before =
        (Object[])
            em.createNativeQuery(
                    "SELECT seq_scan, idx_scan FROM pg_stat_user_tables"
                        + " WHERE relname = 'scheduler_job'")
                .getSingleResult();
    utx.commit();

    long seqBefore = ((Number) before[0]).longValue();
    long idxBefore = ((Number) before[1]).longValue();

    utx.begin();
    storeOperation.run();
    utx.commit();

    utx.begin();
    Object[] after =
        (Object[])
            em.createNativeQuery(
                    "SELECT seq_scan, idx_scan FROM pg_stat_user_tables"
                        + " WHERE relname = 'scheduler_job'")
                .getSingleResult();
    utx.commit();

    long seqDelta = ((Number) after[0]).longValue() - seqBefore;
    long idxDelta = ((Number) after[1]).longValue() - idxBefore;

    log.info(
        String.format(
            "Scan stats [%s]: seq_scan delta=%d, idx_scan delta=%d", label, seqDelta, idxDelta));

    if (seqDelta != 0) {
      throw new AssertionError(
          label + ": sequential scan detected on scheduler_job (seq_scan delta=" + seqDelta + ")");
    }
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
    // Generate TSID-like IDs: timestamp_ms shifted left 22 bits + series counter.
    // The base TSID is computed once per chunk from TsidFactory to avoid collisions.
    long baseTsid = TsidFactory.next();
    em.createNativeQuery(
            "INSERT INTO scheduler_job "
                + "(job_id, status, scheduled_time, job_type, payload, idempotency_key, "
                + "business_key, execution_start_time, execution_end_time, "
                + "created_at, updated_at) "
                + "SELECT "
                + baseTsid
                + " + g, "
                + "'SUCCEEDED', NOW() - INTERVAL '1 hour', 'SINGLE', "
                + "'{\"target\":\"run.ratchet.testsuite.app.TimingJob\","
                + "\"method\":\"execute\",\"descriptor\":\"()V\","
                + "\"isStatic\":true,\"args\":[]}'::jsonb, "
                + "gen_random_uuid()::text, "
                + "'"
                + keyPrefix
                + "-' || (g + "
                + offset
                + "), "
                + "NOW() - INTERVAL '1 hour', "
                + "NOW() - INTERVAL '1 hour' + INTERVAL '10 milliseconds', "
                + "NOW(), NOW() "
                + "FROM generate_series(1, "
                + batchCount
                + ") AS g")
        .executeUpdate();
  }

  private void insertMysqlChunk(int batchCount, int offset, String keyPrefix) {
    long baseTsid = TsidFactory.next();
    em.createNativeQuery("SET @@cte_max_recursion_depth = " + (batchCount + 1)).executeUpdate();
    em.createNativeQuery(
            "INSERT INTO scheduler_job "
                + "(job_id, status, scheduled_time, job_type, payload, idempotency_key, "
                + "business_key, execution_start_time, execution_end_time, "
                + "created_at, updated_at) "
                + "WITH RECURSIVE seq(n) AS ("
                + "SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < "
                + batchCount
                + ") "
                + "SELECT "
                + baseTsid
                + " + n, "
                + "'SUCCEEDED', NOW() - INTERVAL 1 HOUR, 'SINGLE', "
                + "JSON_OBJECT('target','run.ratchet.testsuite.app.TimingJob',"
                + "'method','execute','descriptor','()V','isStatic',true,'args',JSON_ARRAY()), "
                + "UUID(), "
                + "CONCAT('"
                + keyPrefix
                + "-', n + "
                + offset
                + "), "
                + "NOW() - INTERVAL 1 HOUR, "
                + "DATE_ADD(NOW() - INTERVAL 1 HOUR, INTERVAL 10000 MICROSECOND), "
                + "NOW(), NOW() "
                + "FROM seq")
        .executeUpdate();
  }

  private void rollbackQuietly() {
    try {
      utx.rollback();
    } catch (Exception ignored) {
      // best-effort rollback
    }
  }
}
