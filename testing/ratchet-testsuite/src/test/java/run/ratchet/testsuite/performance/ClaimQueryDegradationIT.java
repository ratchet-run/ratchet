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
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import run.ratchet.testsuite.util.PollerControl;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Measures raw claim query latency in isolation at different table sizes. Unlike {@link
 * TableGrowthDegradationIT} which measures end-to-end throughput, this test bypasses the scheduler
 * and times {@code countReadyJobs()} directly to isolate the query cost.
 *
 * <p>Uses a single test method with multiple phases to avoid {@code @BeforeEach} truncation.
 */
class ClaimQueryDegradationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ClaimQueryDegradationIT.class.getName());

  @BeforeEach
  void stopPollerForIsolatedMeasurement() {
    // This IT bypasses the scheduler and times the claim query directly. Runs after the base
    // class's @BeforeEach starts the poller, so it leaves the poller quiesced for the test body.
    // Two reasons: the live poller would race the measured rows, and because
    // pg_stat_user_tables.seq_scan is a process-global counter, any poll tick landing inside an
    // assertNoFullScan window would inflate the seq_scan delta and flake the assertion.
    PollerControl.stopAndAwait(pollerScheduler);
  }

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
  void claimQueryLatencyVsTableSize() {
    int[] tableSizes = {0, 5000, 10_000, 100_000, 1_000_000};
    int iterations = 100;
    long baselineP99 = 0;

    int previousSize = 0;
    for (int tableSize : tableSizes) {
      try {
        // Insert background rows to reach target
        int toInsert = tableSize - previousSize;
        if (toInsert > 0) {
          perfHelper.insertPendingBackgroundRows(toInsert, previousSize, "bg-claim");
          log.info("Inserted " + toInsert + " background rows (total target: " + tableSize + ")");
        }
        previousSize = tableSize;

        // Warmup queries
        Instant now = Instant.now();
        for (int w = 0; w < 20; w++) {
          jobAnalyticsStore.countReadyJobs(now);
        }

        // Measured queries
        long[] times =
            measureQueryTimes(iterations, () -> jobAnalyticsStore.countReadyJobs(Instant.now()));

        long[] percentiles = computePercentiles(times, 0.50, 0.95, 0.99);
        String sizeKey = formatSizeKey(tableSize);

        log.info(
            String.format(
                "Claim query [%s rows]: p50=%dms, p95=%dms, p99=%dms",
                sizeKey, percentiles[0], percentiles[1], percentiles[2]));

        reportWriter()
            .addReport(
                new PerformanceReport(
                    "claimQuery." + sizeKey,
                    iterations,
                    0,
                    0,
                    percentiles[0],
                    percentiles[1],
                    percentiles[2]));

        baseline().assertLatencyWithinTolerance("claimQuery." + sizeKey + ".p99Ms", percentiles[2]);

        if (tableSize == 0) {
          baselineP99 = percentiles[2];
        }
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "Claim query measurement phase failed at tableSize=" + tableSize, e);
      }
    }

    // Degradation ratio (largest vs 0K)
    String lastSizeKey = formatSizeKey(tableSizes[tableSizes.length - 1]);
    try {
      if (baselineP99 > 0) {
        long finalP99 =
            computePercentiles(
                measureQueryTimes(
                    iterations, () -> jobAnalyticsStore.countReadyJobs(Instant.now())),
                0.99)[0];
        double degradationRatio = (double) finalP99 / baselineP99;
        log.info(
            String.format(
                "Claim query degradation ratio (%s/0K): %.2f", lastSizeKey, degradationRatio));
        baseline().assertLatencyWithinTolerance("claimQuery.degradationRatio", degradationRatio);
      }
    } catch (AssertionError | RuntimeException e) {
      throw new AssertionError(
          "Claim query degradation ratio phase failed at tableSize=" + lastSizeKey, e);
    }

    // Verify the claim path uses an index scan on the hot queue at maximum table size.
    // claimNextBatchOptimized supplies job_type (the leading column of idx_claim_executable), so it
    // exercises the real claim access path. countReadyJobs omits job_type and serves only as the
    // latency proxy above, not a scan probe. The background rows are far-future, so the claim finds
    // no due rows and ranges over the index without a sequential scan.
    try {
      perfHelper.assertNoFullScan(
          "scheduler_job_queue",
          "claimNextBatch @ " + lastSizeKey,
          () ->
              jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "perf-test-node"));
    } catch (AssertionError | RuntimeException e) {
      throw new AssertionError(
          "Claim query full-scan verification phase failed at tableSize=" + lastSizeKey, e);
    }
  }
}
