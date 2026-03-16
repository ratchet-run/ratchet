package run.ratchet.testsuite.performance;

import static org.awaitility.Awaitility.await;

import run.ratchet.api.JobHandle;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.PerformanceTestHelper;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

/**
 * Base class for performance integration tests. Provides longer Awaitility defaults, job enqueue
 * helpers, percentile computation, and access to baseline/reporting utilities.
 *
 * <p>Store-specific bulk operations (background row insertion, scan diagnostics) are delegated to
 * {@link PerformanceTestHelper}, which is resolved by CDI based on the active store backend.
 *
 * <p>All performance test classes should extend this and use the {@code @Tag("performance")}
 * annotation (inherited from this class).
 */
@Tag("performance")
public abstract class BasePerformanceIT extends BaseRatchetIT {

  protected static final Duration PERF_TIMEOUT = Duration.ofSeconds(180);
  protected static final Duration PERF_POLL_INTERVAL = Duration.ofMillis(200);

  @Inject protected TestJobService jobService;
  @Inject protected JobCrudStore jobCrudStore;
  @Inject protected JobClaimStore jobClaimStore;
  @Inject protected PollerScheduler pollerScheduler;
  @Inject protected PerformanceTestHelper perfHelper;

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
}
