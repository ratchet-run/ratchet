package run.ratchet.testsuite.performance;

import static org.awaitility.Awaitility.await;

import run.ratchet.api.JobHandle;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.api.JobStatus;
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

/** Shared setup for performance ITs. */
@Tag("performance")
public abstract class BasePerformanceIT extends BaseRatchetIT {

  protected static final Duration PERF_TIMEOUT = Duration.ofSeconds(180);
  protected static final Duration PERF_POLL_INTERVAL = Duration.ofMillis(200);

  @Inject protected TestJobService jobService;
  @Inject protected JobCrudStore jobCrudStore;
  @Inject protected JobClaimStore jobClaimStore;
  @Inject protected PollerScheduler pollerScheduler;
  @Inject protected PerformanceTestHelper perfHelper;

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

  protected static long[] computePercentiles(long[] data, double... percentiles) {
    Arrays.sort(data);
    long[] result = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      int index = (int) Math.ceil(percentiles[i] * data.length) - 1;
      result[i] = data[Math.max(0, index)];
    }
    return result;
  }

  protected static String formatSizeKey(int tableSize) {
    if (tableSize >= 1_000_000 && tableSize % 1_000_000 == 0) {
      return (tableSize / 1_000_000) + "M";
    }
    return (tableSize / 1000) + "K";
  }

  @BeforeEach
  void restartPoller() {
    pollerScheduler.start();
    pollerScheduler.wakeup();
  }

  /** Stop poller to avoid TRUNCATE deadlock. */
  @Override
  protected void truncateAll() throws Exception {
    pollerScheduler.stop();
    // Brief pause to let any in-flight poll cycle complete its transaction
    Thread.sleep(100);
    super.truncateAll();
  }

  protected List<JobHandle> enqueueN(int count, SerializableCheckedRunnable task) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueueNow(task));
    }
    return handles;
  }

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

  protected long[] queryQueueWaitPercentiles(double... percentiles) {
    long[] result = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      result[i] = jobCrudStore.getQueueWaitTimePercentile(percentiles[i]);
    }
    return result;
  }

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

  protected List<JobHandle> enqueueNWithRetries(
      int count, SerializableCheckedRunnable task, int maxRetries) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueue(task).withMaxRetries(maxRetries).submit());
    }
    return handles;
  }
}
