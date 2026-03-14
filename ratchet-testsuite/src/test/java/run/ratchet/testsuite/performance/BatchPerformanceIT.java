package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Measures batch processing overhead and scaling across different batch sizes. */
class BatchPerformanceIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(BatchPerformanceIT.class.getName());
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
            PerformanceMetricsCollector.class,
            TestJobService.class,
            BasePerformanceIT.class,
            PerformanceBaseline.class,
            PerformanceReport.class,
            PerformanceReportWriter.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounters() {
    TimingJob.resetCount();
    PerformanceMetricsCollector.reset();
  }

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
  }

  @Test
  void batchOverhead_50items() {
    measureBatch(50);
  }

  @Test
  void batchOverhead_200items() {
    measureBatch(200);
  }

  @Test
  void batchOverhead_500items() {
    measureBatch(500);
  }

  private void measureBatch(int batchSize) {
    String batchSizes = System.getProperty("perf.batch.sizes", "50,200,500");
    if (!batchSizes.contains(String.valueOf(batchSize))) {
      log.info(
          "Skipping batch size " + batchSize + " (not in perf.batch.sizes=" + batchSizes + ")");
      return;
    }

    // Warmup with a small batch
    List<String> warmupItems = generateItems(10);
    JobHandle warmupHandle =
        jobService
            .enqueueBatch("warmup-batch")
            .forEach(warmupItems, TimingJob::processBatchItem)
            .submit();
    JobAssertions.assertBatchSucceeded(jobCrudStore, warmupHandle, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    // Measured batch
    List<String> items = generateItems(batchSize);
    log.info("Measuring batch overhead: " + batchSize + " items");

    long startMs = System.currentTimeMillis();
    JobHandle batchHandle =
        jobService
            .enqueueBatch("perf-batch-" + batchSize)
            .forEach(items, TimingJob::processBatchItem)
            .submit();
    JobAssertions.assertBatchSucceeded(jobCrudStore, batchHandle, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    PerformanceMetricsCollector.PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();
    double throughput = (batchSize * 1000.0) / totalMs;

    log.info(
        String.format(
            "Batch[%d]: %.1f jobs/sec, total=%dms, p50=%dms, p95=%dms, p99=%dms",
            batchSize, throughput, totalMs, snap.p50Ms(), snap.p95Ms(), snap.p99Ms()));

    assertTrue(
        snap.completedCount() >= batchSize,
        "Expected at least " + batchSize + " completed jobs but got " + snap.completedCount());

    PerformanceReport report =
        new PerformanceReport(
            "batch." + batchSize,
            batchSize,
            totalMs,
            throughput,
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter.addReport(report);

    baseline.assertWithinTolerance("batch." + batchSize + ".throughputJobsPerSec", throughput);
    baseline.assertLatencyWithinTolerance("batch." + batchSize + ".totalTimeMs", totalMs);
  }

  private static List<String> generateItems(int count) {
    List<String> items = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      items.add("item-" + i);
    }
    return items;
  }
}
