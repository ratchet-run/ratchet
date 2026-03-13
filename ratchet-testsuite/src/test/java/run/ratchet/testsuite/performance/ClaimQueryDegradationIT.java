package run.ratchet.testsuite.performance;

import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import java.time.Instant;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Measures raw claim query latency in isolation at different table sizes. Unlike {@link
 * TableGrowthDegradationIT} which measures end-to-end throughput, this test bypasses the scheduler
 * and times {@code countReadyJobs()} directly to isolate the query cost.
 *
 * <p>Uses a single test method with multiple phases to avoid {@code @BeforeEach} truncation.
 */
class ClaimQueryDegradationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ClaimQueryDegradationIT.class.getName());
  private static final PerformanceBaseline baseline = createBaseline();
  private static final PerformanceReportWriter reportWriter = createReportWriter();

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
            TestJobService.class,
            BasePerformanceIT.class,
            PerformanceBaseline.class,
            PerformanceReport.class,
            PerformanceReportWriter.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
  }

  @Test
  void claimQueryLatencyVsTableSize() throws Exception {
    int[] tableSizes = {0, 5000, 10_000, 100_000, 1_000_000};
    int iterations = 100;
    long baselineP99 = 0;

    int previousSize = 0;
    for (int tableSize : tableSizes) {
      // Insert background rows to reach target
      int toInsert = tableSize - previousSize;
      if (toInsert > 0) {
        insertBackgroundRowsNative(toInsert, "bg-claim");
        log.info("Inserted " + toInsert + " background rows (total target: " + tableSize + ")");
      }
      previousSize = tableSize;

      // Warmup queries
      Instant now = Instant.now();
      for (int w = 0; w < 20; w++) {
        utx.begin();
        jobCrudStore.countReadyJobs(now);
        utx.commit();
      }

      // Measured queries
      long[] times = new long[iterations];
      for (int i = 0; i < iterations; i++) {
        utx.begin();
        long start = System.nanoTime();
        jobCrudStore.countReadyJobs(Instant.now());
        long elapsed = System.nanoTime() - start;
        times[i] = elapsed / 1_000_000;
        utx.commit();
      }

      long[] percentiles = computePercentiles(times, 0.50, 0.95, 0.99);
      String sizeKey = formatSizeKey(tableSize);

      log.info(
          String.format(
              "Claim query [%s rows]: p50=%dms, p95=%dms, p99=%dms",
              sizeKey, percentiles[0], percentiles[1], percentiles[2]));

      reportWriter.addReport(
          new PerformanceReport(
              "claimQuery." + sizeKey,
              iterations,
              0,
              0,
              percentiles[0],
              percentiles[1],
              percentiles[2]));

      baseline.assertLatencyWithinTolerance("claimQuery." + sizeKey + ".p99Ms", percentiles[2]);

      if (tableSize == 0) {
        baselineP99 = percentiles[2];
      }
    }

    // Degradation ratio (largest vs 0K)
    String lastSizeKey = formatSizeKey(tableSizes[tableSizes.length - 1]);
    if (baselineP99 > 0) {
      long finalP99 = computePercentiles(measureQueryTimes(iterations), 0.99)[0];
      double degradationRatio = (double) finalP99 / baselineP99;
      log.info(
          String.format(
              "Claim query degradation ratio (%s/0K): %.2f", lastSizeKey, degradationRatio));
      baseline.assertLatencyWithinTolerance("claimQuery.degradationRatio", degradationRatio);
    }

    // Verify actual store methods use index scans at maximum table size
    Instant finalNow = Instant.now();
    assertNoFullTableScan(
        "countReadyJobs @ " + lastSizeKey, () -> jobCrudStore.countReadyJobs(finalNow));

    assertNoFullTableScan(
        "claimNextBatch @ " + lastSizeKey,
        () -> jobClaimStore.claimNextBatchOptimized(10, "perf-test-node"));
  }

  private long[] measureQueryTimes(int iterations) throws Exception {
    long[] times = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      utx.begin();
      long start = System.nanoTime();
      jobCrudStore.countReadyJobs(Instant.now());
      long elapsed = System.nanoTime() - start;
      times[i] = elapsed / 1_000_000;
      utx.commit();
    }
    return times;
  }
}
