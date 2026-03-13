package run.ratchet.testsuite.performance;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.JobHandle;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

/**
 * Base class for performance integration tests. Provides longer Awaitility defaults, job enqueue
 * helpers, percentile computation, and access to baseline/reporting utilities.
 *
 * <p>All performance test classes should extend this and use the {@code @Tag("performance")}
 * annotation (inherited from this class).
 */
@Tag("performance")
public abstract class BasePerformanceIT extends BaseRatchetIT {

  private static final Logger log = Logger.getLogger(BasePerformanceIT.class.getName());

  protected static final Duration PERF_TIMEOUT = Duration.ofSeconds(120);
  protected static final Duration PERF_POLL_INTERVAL = Duration.ofMillis(200);

  @Inject protected TestJobService jobService;
  @Inject protected JobCrudStore jobCrudStore;
  @Inject protected JobClaimStore jobClaimStore;
  @Inject protected PollerScheduler pollerScheduler;

  /**
   * Stops the poller before truncation to prevent lock conflicts. Both PostgreSQL (ACCESS EXCLUSIVE
   * for TRUNCATE) and MySQL (metadata lock for TRUNCATE) deadlock with the poller's concurrent
   * SELECT FOR UPDATE SKIP LOCKED queries. Stopping the poller ensures no active transactions block
   * the cleanup.
   */
  @Override
  protected void truncateAll() throws Exception {
    pollerScheduler.stop();
    // Brief pause to let any in-flight poll cycle complete its transaction
    Thread.sleep(100);
    super.truncateAll();
  }

  /** Restarts the poller after truncation to ensure it is active for the test. */
  @BeforeEach
  void restartPoller() {
    pollerScheduler.start();
    pollerScheduler.wakeup();
  }

  protected static int getWarmupCount() {
    return Integer.getInteger("perf.warmup.count", 50);
  }

  protected static int getMeasuredCount() {
    return Integer.getInteger("perf.measured.count", 500);
  }

  protected static double getTolerance() {
    return Double.parseDouble(System.getProperty("perf.tolerance", "0.20"));
  }

  protected static String getDbType() {
    return System.getProperty("ratchet.test.db.type", "mysql");
  }

  protected static PerformanceBaseline createBaseline() {
    String dbType = getDbType();
    double tolerance = getTolerance();
    String baselineDir =
        System.getProperty("perf.baseline.dir", "src/test/resources/perf-baselines");
    return new PerformanceBaseline(dbType, tolerance, baselineDir);
  }

  protected static PerformanceReportWriter createReportWriter() {
    return new PerformanceReportWriter(getDbType());
  }

  /**
   * Enqueues N jobs with the given task and returns their handles.
   *
   * @param count number of jobs to enqueue
   * @param task the job logic to execute
   * @return list of job handles
   */
  protected List<JobHandle> enqueueN(int count, SerializableCheckedRunnable task) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueueNow(task));
    }
    return handles;
  }

  /**
   * Waits for all jobs identified by the given handles to reach SUCCEEDED status.
   *
   * @param handles the job handles to wait for
   * @param timeout maximum wait duration
   */
  protected void awaitAllCompleted(List<JobHandle> handles, Duration timeout) {
    await()
        .atMost(timeout)
        .pollInterval(PERF_POLL_INTERVAL)
        .untilAsserted(
            () -> {
              for (JobHandle handle : handles) {
                JobStatus status = jobCrudStore.getJobStatus(handle.id());
                if (status != JobStatus.SUCCEEDED) {
                  throw new AssertionError(
                      "Job " + handle.id() + " expected SUCCEEDED but was " + status);
                }
              }
            });
  }

  /**
   * Computes percentile values from an array of longs.
   *
   * @param data the raw data (will be sorted in place)
   * @param percentiles the percentile fractions to compute (e.g., 0.50, 0.95, 0.99)
   * @return array of percentile values in the same order as the percentiles parameter
   */
  protected static long[] computePercentiles(long[] data, double... percentiles) {
    Arrays.sort(data);
    long[] result = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      int index = (int) Math.ceil(percentiles[i] * data.length) - 1;
      result[i] = data[Math.max(0, index)];
    }
    return result;
  }

  /**
   * Queries queue wait time percentiles from the store.
   *
   * @param percentiles the percentile fractions to query
   * @return array of percentile values in milliseconds
   */
  protected long[] queryQueueWaitPercentiles(double... percentiles) {
    long[] result = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      result[i] = jobCrudStore.getQueueWaitTimePercentile(percentiles[i]);
    }
    return result;
  }

  /**
   * Waits for all jobs identified by the given handles to reach a terminal status (SUCCEEDED,
   * FAILED, or CANCELED). Unlike {@link #awaitAllCompleted}, this method accepts any terminal
   * state, which is needed for tests where some jobs are expected to fail.
   *
   * @param handles the job handles to wait for
   * @param timeout maximum wait duration
   */
  protected void awaitAllTerminal(List<JobHandle> handles, Duration timeout) {
    await()
        .atMost(timeout)
        .pollInterval(PERF_POLL_INTERVAL)
        .untilAsserted(
            () -> {
              for (JobHandle handle : handles) {
                JobStatus status = jobCrudStore.getJobStatus(handle.id());
                if (status != JobStatus.SUCCEEDED
                    && status != JobStatus.FAILED
                    && status != JobStatus.CANCELED) {
                  throw new AssertionError(
                      "Job " + handle.id() + " expected terminal but was " + status);
                }
              }
            });
  }

  /**
   * Enqueues N jobs with the given task and configures each with the specified max retries.
   *
   * @param count number of jobs to enqueue
   * @param task the job logic to execute
   * @param maxRetries maximum retry attempts per job
   * @return list of job handles
   */
  protected List<JobHandle> enqueueNWithRetries(
      int count, SerializableCheckedRunnable task, int maxRetries) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueue(task).withMaxRetries(maxRetries).submit());
    }
    return handles;
  }

  /**
   * Inserts background SUCCEEDED rows using native SQL, bypassing JPA entirely. Uses {@code
   * generate_series()} on PostgreSQL and recursive CTEs on MySQL to generate all rows server-side
   * in a single statement per batch.
   *
   * <p>For large counts, rows are inserted in chunks of 100K to avoid transaction timeouts.
   *
   * @param count the number of background rows to insert
   * @param keyPrefix prefix for business_key values (e.g., "bg-growth", "bg-claim")
   */
  protected void insertBackgroundRowsNative(int count, String keyPrefix) throws Exception {
    String dbType = getDbType();
    int chunkSize = 100_000;

    for (int offset = 0; offset < count; offset += chunkSize) {
      int batchCount = Math.min(chunkSize, count - offset);

      utx.begin();
      if ("postgresql".equals(dbType)) {
        em.createNativeQuery(
                "INSERT INTO scheduler_job "
                    + "(status, scheduled_time, job_type, payload, idempotency_key, "
                    + "business_key, execution_start_time, execution_end_time, "
                    + "created_at, updated_at) "
                    + "SELECT 'SUCCEEDED', NOW() - INTERVAL '1 hour', 'SINGLE', "
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
      } else {
        // MySQL: recursive CTE
        em.createNativeQuery("SET @@cte_max_recursion_depth = " + (batchCount + 1)).executeUpdate();
        em.createNativeQuery(
                "INSERT INTO scheduler_job "
                    + "(status, scheduled_time, job_type, payload, idempotency_key, "
                    + "business_key, execution_start_time, execution_end_time, "
                    + "created_at, updated_at) "
                    + "WITH RECURSIVE seq(n) AS ("
                    + "SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < "
                    + batchCount
                    + ") "
                    + "SELECT 'SUCCEEDED', NOW() - INTERVAL 1 HOUR, 'SINGLE', "
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
      utx.commit();

      if (count > chunkSize) {
        log.info(
            String.format("  ... inserted %d / %d background rows", offset + batchCount, count));
      }
    }

    // Refresh table statistics so the query planner has accurate selectivity estimates.
    // In production, autovacuum/autoanalyze handles this automatically.
    utx.begin();
    if ("postgresql".equals(dbType)) {
      em.createNativeQuery("ANALYZE scheduler_job").executeUpdate();
    } else {
      // MySQL ANALYZE TABLE returns a result set, not an update count
      em.createNativeQuery("ANALYZE TABLE scheduler_job").getResultList();
    }
    utx.commit();
  }

  /**
   * Formats a table size as a human-readable key (e.g., 1000 → "1K", 1000000 → "1M").
   *
   * @param tableSize the row count
   * @return formatted size key
   */
  protected static String formatSizeKey(int tableSize) {
    if (tableSize >= 1_000_000 && tableSize % 1_000_000 == 0) {
      return (tableSize / 1_000_000) + "M";
    }
    return (tableSize / 1000) + "K";
  }

  /**
   * Executes a store operation and asserts that no sequential/full table scan occurred on
   * scheduler_job. Verifies the actual store implementation uses index scans by measuring
   * database-level scan counters before and after the operation.
   *
   * <p>On PostgreSQL, uses {@code pg_stat_user_tables} (seq_scan / idx_scan deltas) which are
   * per-table. On MySQL, {@code Select_scan} is session-wide across all tables, so we log the delta
   * for informational purposes but only assert on PostgreSQL where per-table metrics are available.
   *
   * @param label descriptive label for log output and assertion messages
   * @param storeOperation the actual store method call to verify
   */
  @SuppressWarnings("unchecked")
  protected void assertNoFullTableScan(String label, Runnable storeOperation) throws Exception {
    String dbType = getDbType();

    if ("postgresql".equals(dbType)) {
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

      // idx_scan=0 with seq_scan=0 is valid — it means an Index Only Scan was used,
      // which is even more efficient as it doesn't touch the heap table at all.
      log.info(
          String.format(
              "Scan stats [%s]: seq_scan delta=%d, idx_scan delta=%d", label, seqDelta, idxDelta));

      assertEquals(
          0,
          seqDelta,
          label + ": sequential scan detected on scheduler_job (seq_scan delta=" + seqDelta + ")");

    } else {
      // MySQL: Select_scan is session-wide (all tables), not per-table like PostgreSQL's
      // pg_stat_user_tables. JPA/Hibernate internal queries, metadata lookups, and even
      // SHOW STATUS itself can increment this counter. We log the delta for informational
      // purposes but cannot reliably assert on it for a specific table.
      utx.begin();
      storeOperation.run();
      utx.commit();

      log.info(
          String.format(
              "Scan stats [%s]: MySQL — per-table scan metrics not available, "
                  + "relying on index definitions and EXPLAIN plans for verification",
              label));
    }
  }
}
