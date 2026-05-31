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

import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceReport;

/**
 * Measures throughput scaling characteristics under the default thread pool configuration.
 *
 * <p>Rather than attempting to reconfigure the thread pool per test (which would require separate
 * WildFly instances or redeployments), this test measures throughput with varying job counts and
 * concurrency patterns using the default thread pool. The results provide a scaling baseline that
 * can be compared across different default pool sizes configured via environment variables.
 *
 * <p>To test different pool sizes, run the performance suite with different {@code
 * SCHEDULER_THREAD_POOL_SIZE_DEFAULT} values:
 *
 * <pre>
 * SCHEDULER_THREAD_POOL_SIZE_DEFAULT=2 mvn verify -Pwildfly-managed,postgresql,performance
 * SCHEDULER_THREAD_POOL_SIZE_DEFAULT=10 mvn verify -Pwildfly-managed,postgresql,performance
 * </pre>
 */
class ConcurrentScalingIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ConcurrentScalingIT.class.getName());

  @Deployment
  public static WebArchive createDeployment() {
    return createPerformanceDeployment(ConfigurableWorkJob.class);
  }

  @BeforeEach
  void resetCounters() {
    TimingJob.resetCount();
    ConfigurableWorkJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @Test
  void noOpScalingBaseline() {
    int measured = getMeasuredCount();

    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    // Measured burst enqueue — all jobs at once to saturate the thread pool
    log.info("Scaling baseline: burst-enqueuing " + measured + " no-op jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    double throughput = (measured * 1000.0) / totalMs;
    PerformanceMetricsCollector.PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();

    log.info(
        String.format(
            "Scaling baseline (no-op): %.1f jobs/sec (%d jobs in %d ms)",
            throughput, measured, totalMs));

    PerformanceReport report =
        new PerformanceReport(
            "scaling.noOp.burst",
            measured,
            totalMs,
            throughput,
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter().addReport(report);
    assertSnapshotCompleted("scaling.noOp.burst", snap, measured);
    assertLatencyPercentilesSane("scaling.noOp.burst", snap, maxP50Ms(), maxP95Ms(), maxP99Ms());
    baseline().assertWithinTolerance("scaling.noOp.burst.jobsPerSec", throughput);
  }

  @Test
  void workloadScalingBaseline() {
    int measured = getMeasuredCount();
    ConfigurableWorkJob.setSleepMs(10);

    List<JobHandle> warmup = enqueueN(getWarmupCount(), ConfigurableWorkJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    ConfigurableWorkJob.reset();
    ConfigurableWorkJob.setSleepMs(10);

    // Measured burst
    log.info("Scaling baseline: burst-enqueuing " + measured + " 10ms-work jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, ConfigurableWorkJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    double throughput = (measured * 1000.0) / totalMs;
    PerformanceMetricsCollector.PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();

    log.info(
        String.format(
            "Scaling baseline (10ms work): %.1f jobs/sec (%d jobs in %d ms)",
            throughput, measured, totalMs));

    PerformanceReport report =
        new PerformanceReport(
            "scaling.work10ms.burst",
            measured,
            totalMs,
            throughput,
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter().addReport(report);
    assertSnapshotCompleted("scaling.work10ms.burst", snap, measured);
    assertLatencyPercentilesSane(
        "scaling.work10ms.burst", snap, maxP50Ms(), maxP95Ms(), maxP99Ms());
    baseline().assertWithinTolerance("scaling.work10ms.burst.jobsPerSec", throughput);
  }

  @Test
  void incrementalLoadScaling() {
    int[] loadLevels = {50, 100, 200, 500};

    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    for (int load : loadLevels) {
      PerformanceMetricsCollector.reset();
      TimingJob.resetCount();

      log.info("Incremental load: " + load + " jobs");
      long startMs = System.currentTimeMillis();
      List<JobHandle> handles = enqueueN(load, TimingJob::execute);
      awaitAllCompleted(handles, PERF_TIMEOUT);
      long totalMs = System.currentTimeMillis() - startMs;

      double throughput = (load * 1000.0) / totalMs;
      PerformanceMetricsCollector.PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();

      log.info(String.format("Load[%d]: %.1f jobs/sec, total=%dms", load, throughput, totalMs));

      PerformanceReport report =
          new PerformanceReport(
              "scaling.incremental." + load,
              load,
              totalMs,
              throughput,
              snap.p50Ms(),
              snap.p95Ms(),
              snap.p99Ms());
      reportWriter().addReport(report);
      assertSnapshotCompleted("scaling.incremental." + load, snap, load);
      assertLatencyPercentilesSane(
          "scaling.incremental." + load, snap, maxP50Ms(), maxP95Ms(), maxP99Ms());
      baseline().assertWithinTolerance("scaling.incremental." + load + ".jobsPerSec", throughput);
    }
  }

  private static long maxP50Ms() {
    return Long.getLong("perf.scaling.max.p50.ms", 5_000L);
  }

  private static long maxP95Ms() {
    return Long.getLong("perf.scaling.max.p95.ms", 10_000L);
  }

  private static long maxP99Ms() {
    return Long.getLong("perf.scaling.max.p99.ms", 30_000L);
  }
}
