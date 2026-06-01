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
package run.ratchet.testsuite.performance;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import org.awaitility.core.ConditionTimeoutException;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.PerformanceTestHelper;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestMetricsCollectorAdapter;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Shared setup for performance ITs. */
@Tag("performance")
public abstract class BasePerformanceIT extends BaseRatchetIT {

  protected static final Duration PERF_TIMEOUT = Duration.ofSeconds(180);

  /**
   * Poll interval used when waiting for job completion. Defaults to 200ms, but can be overridden
   * via the {@code perf.poll.interval.ms} system property to reduce flakiness on slow CI hosts
   * where JVM scheduling delays can cause polling to miss the completion window.
   */
  protected static final Duration PERF_POLL_INTERVAL =
      Duration.ofMillis(Long.getLong("perf.poll.interval.ms", 200));

  private static final Duration POLLER_STOP_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration POLLER_STOP_POLL_INTERVAL = Duration.ofMillis(10);
  private static final ConcurrentMap<Class<?>, PerformanceBaseline> BASELINES =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<Class<?>, PerformanceReportWriter> REPORT_WRITERS =
      new ConcurrentHashMap<>();

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

  protected final PerformanceBaseline baseline() {
    return BASELINES.computeIfAbsent(getClass(), ignored -> createBaseline());
  }

  protected final PerformanceReportWriter reportWriter() {
    return REPORT_WRITERS.computeIfAbsent(getClass(), ignored -> createReportWriter());
  }

  @AfterEach
  void writePerformanceResults() {
    reportWriter().writeClassFragment(getClass().getSimpleName());
    baseline().writeRecordedBaselines();
  }

  protected static WebArchive createPerformanceDeployment(Class<?>... additionalClasses) {
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    List<Class<?>> classes = new ArrayList<>();
    classes.add(TimingJob.class);
    classes.add(PerformanceMetricsCollector.class);
    classes.add(TestMetricsCollectorAdapter.class);
    classes.add(TestJobService.class);
    classes.add(BasePerformanceIT.class);
    classes.add(PerformanceBaseline.class);
    classes.add(PerformanceReport.class);
    classes.add(PerformanceReportWriter.class);
    classes.addAll(List.of(additionalClasses));

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, getDbType())
        .addClasses(classes.toArray(new Class<?>[0]))
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
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

  protected static List<String> generateItems(int count) {
    List<String> items = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      items.add("item-" + i);
    }
    return items;
  }

  protected static long[] measureQueryTimes(int iterations, LongSupplier operation) {
    long[] times = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      operation.getAsLong();
      long elapsed = System.nanoTime() - start;
      times[i] = elapsed / 1_000_000;
    }
    return times;
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
    await()
        .atMost(POLLER_STOP_TIMEOUT)
        .pollInterval(POLLER_STOP_POLL_INTERVAL)
        .until(() -> pollerSchedulerStopped(pollerScheduler));
    super.truncateAll();
  }

  static boolean pollerSchedulerStopped(PollerScheduler scheduler) {
    Object lock = readField(scheduler, "scheduleLock", Object.class);
    synchronized (lock) {
      Future<?> handle = readField(scheduler, "handle", Future.class);
      return !readField(scheduler, "cycleRunning", Boolean.class)
          && (handle == null || handle.isDone() || handle.isCancelled());
    }
  }

  private static <T> T readField(Object target, String name, Class<T> type) {
    try {
      Field field = DefaultPollerScheduler.class.getDeclaredField(name);
      field.setAccessible(true);
      return type.cast(field.get(target));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to read " + name + " from " + target.getClass(), e);
    }
  }

  protected List<JobHandle> enqueueN(int count, SerializableCheckedRunnable task) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueueNow(task));
    }
    return handles;
  }

  /**
   * Waits until every submitted job succeeds. Use this for performance paths where failures should
   * fail the test instead of merely ending execution.
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

  protected long[] queryQueueWaitPercentiles(double... percentiles) {
    long[] result = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      result[i] = jobCrudStore.getQueueWaitTimePercentile(percentiles[i]);
    }
    return result;
  }

  /**
   * Waits until every submitted job reaches an end-of-life state, whether it succeeded, failed, or
   * was canceled. Use this for tests that intentionally exercise terminal failure or cancellation.
   */
  protected void awaitAllTerminal(List<JobHandle> handles, Duration timeout) {
    try {
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
    } catch (ConditionTimeoutException e) {
      throw new AssertionError(
          "Timed out waiting for "
              + handles.size()
              + " jobs to reach terminal status. Status histogram: "
              + statusHistogram(handles),
          e);
    }
  }

  private Map<JobStatus, Integer> statusHistogram(List<JobHandle> handles) {
    Map<JobStatus, Integer> histogram = new EnumMap<>(JobStatus.class);
    for (JobHandle handle : handles) {
      JobStatus status = jobCrudStore.getJobStatus(handle.id());
      histogram.merge(status, 1, Integer::sum);
    }
    return histogram;
  }

  protected List<JobHandle> enqueueNWithRetries(
      int count, SerializableCheckedRunnable task, int maxRetries) {
    List<JobHandle> handles = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      handles.add(jobService.enqueue(task).withMaxRetries(maxRetries).submit());
    }
    return handles;
  }

  protected void assertSnapshotCompleted(
      String scenario,
      PerformanceMetricsCollector.PerformanceSnapshot snap,
      long expectedCompleted) {
    assertEquals(
        expectedCompleted,
        snap.completedCount(),
        scenario
            + " completed count mismatch: expected "
            + expectedCompleted
            + " got "
            + snap.completedCount());
    assertTrue(
        snap.startedCount() >= expectedCompleted,
        scenario
            + " should have at least "
            + expectedCompleted
            + " started jobs but had "
            + snap.startedCount());
  }

  protected void assertLatencyPercentilesSane(
      String scenario,
      PerformanceMetricsCollector.PerformanceSnapshot snap,
      long maxP50Ms,
      long maxP95Ms,
      long maxP99Ms) {
    assertTrue(
        snap.p50Ms() >= 0 && snap.p50Ms() <= maxP50Ms,
        scenario + " p50 latency unreasonable: " + snap.p50Ms() + "ms");
    assertTrue(
        snap.p95Ms() >= snap.p50Ms() && snap.p95Ms() <= maxP95Ms,
        scenario
            + " p95 latency unreasonable: p50="
            + snap.p50Ms()
            + "ms p95="
            + snap.p95Ms()
            + "ms");
    assertTrue(
        snap.p99Ms() >= snap.p95Ms() && snap.p99Ms() <= maxP99Ms,
        scenario
            + " p99 latency unreasonable: p95="
            + snap.p95Ms()
            + "ms p99="
            + snap.p99Ms()
            + "ms");
  }

  protected long countHandlesWithStatus(List<JobHandle> handles, JobStatus expectedStatus) {
    long count = 0;
    for (JobHandle handle : handles) {
      if (jobCrudStore.getJobStatus(handle.id()) == expectedStatus) {
        count++;
      }
    }
    return count;
  }
}
