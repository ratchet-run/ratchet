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

import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestMetricsCollectorAdapter;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Measures end-to-end throughput degradation as the scheduler_job table grows with completed rows.
 * The claim query uses {@code SELECT ... FOR UPDATE SKIP LOCKED} which may degrade with table size.
 *
 * <p>Uses a single test method with multiple phases to avoid {@code @BeforeEach} truncation between
 * measurements.
 */
class TableGrowthDegradationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(TableGrowthDegradationIT.class.getName());

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            TimingJob.class,
            ConfigurableWorkJob.class,
            ProbabilisticFailingJob.class,
            PerformanceMetricsCollector.class,
            TestMetricsCollectorAdapter.class,
            TestJobService.class,
            BasePerformanceIT.class,
            PerformanceBaseline.class,
            PerformanceReport.class,
            PerformanceReportWriter.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void throughputDegradationCurve() {
    int measureCount = 100;
    int[] tableSizes = {0, 1000, 5000, 10_000, 100_000, 1_000_000};
    double baselineThroughput = 0;
    double lastThroughput = 0;

    // Warmup
    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    int previousSize = 0;
    for (int tableSize : tableSizes) {
      // Insert background rows to reach the target table size
      int toInsert = tableSize - previousSize;
      if (toInsert > 0) {
        perfHelper.insertTerminalBackgroundRows(toInsert, previousSize, "bg-growth");
        log.info("Inserted " + toInsert + " background rows (total target: " + tableSize + ")");
      }
      previousSize = tableSize;

      // Measure throughput at this table size
      PerformanceMetricsCollector.reset();
      TimingJob.resetCount();

      log.info("Table growth: measuring throughput with " + tableSize + " background rows");
      long startMs = System.currentTimeMillis();
      List<JobHandle> handles = enqueueN(measureCount, TimingJob::execute);
      awaitAllCompleted(handles, PERF_TIMEOUT);
      long totalMs = System.currentTimeMillis() - startMs;

      double throughput = (measureCount * 1000.0) / totalMs;
      lastThroughput = throughput;
      String sizeKey = formatSizeKey(tableSize);

      log.info(
          String.format(
              "Table growth [%s]: %.1f jobs/sec (%d jobs in %dms)",
              sizeKey, throughput, measureCount, totalMs));

      reportWriter()
          .addReport(
              new PerformanceReport(
                  "tableDegradation." + sizeKey, measureCount, totalMs, throughput, 0, 0, 0));
      baseline().assertWithinTolerance("tableDegradation." + sizeKey + ".jobsPerSec", throughput);

      if (tableSize == 0) {
        baselineThroughput = throughput;
      }

      // Also measure claim query latency directly
      long claimP99 = measureClaimQueryLatency(50);
      baseline()
          .assertLatencyWithinTolerance("tableDegradation." + sizeKey + ".claimP99Ms", claimP99);

      if (tableSize == 10_000 || tableSize == 100_000 || tableSize == 1_000_000) {
        assertClaimQueriesUseIndexes(sizeKey);
      }
    }

    // Report throughput ratio (largest vs 0K)
    String lastSizeKey = formatSizeKey(tableSizes[tableSizes.length - 1]);
    if (baselineThroughput > 0 && lastThroughput > 0) {
      double throughputRatio = baselineThroughput / lastThroughput;
      log.info(
          String.format(
              "Table growth throughput ratio (0K/%s): %.2f (1.0 = no degradation)",
              lastSizeKey, throughputRatio));
      baseline()
          .assertLatencyWithinTolerance(
              "tableDegradation." + lastSizeKey + ".throughputRatio", throughputRatio);
    }
  }

  private void assertClaimQueriesUseIndexes(String sizeKey) {
    // The claim path reads only the hot scheduler_job_queue, never the cold scheduler_job this IT
    // grows. Asserting no sequential scan on the COLD table therefore proves the split keeps claim
    // isolated from archive growth — the seq_scan delta is zero by design, and a future change that
    // joined a claim query back to the cold table would trip this guard.
    Instant now = Instant.now();
    perfHelper.assertNoFullScan(
        "scheduler_job",
        "countReadyJobs @ " + sizeKey,
        () -> jobAnalyticsStore.countReadyJobs(now));

    perfHelper.assertNoFullScan(
        "scheduler_job",
        "claimNextBatch @ " + sizeKey,
        () -> jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "perf-test-node"));
  }

  private long measureClaimQueryLatency(int iterations) {
    Instant now = Instant.now();
    long[] times = measureQueryTimes(iterations, () -> jobAnalyticsStore.countReadyJobs(now));

    long[] percentiles = computePercentiles(times, 0.99);
    return percentiles[0];
  }
}
