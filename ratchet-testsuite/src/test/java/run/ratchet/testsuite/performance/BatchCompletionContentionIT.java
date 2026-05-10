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
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Tests batch completion contention by measuring per-child overhead at different batch sizes. When
 * batch children finish concurrently, they race to UPDATE the same scheduler_batch row (atomic
 * progress increment + markBatchCompleteIfReady CAS). This test isolates that contention.
 *
 * <p>Unlike {@link BatchPerformanceIT} which measures throughput, this test measures per-child
 * orchestration overhead and whether it scales linearly (contention) or stays constant.
 */
class BatchCompletionContentionIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(BatchCompletionContentionIT.class.getName());

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
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounters() {
    TimingJob.resetCount();
    PerformanceMetricsCollector.reset();
  }

  @Test
  void completionOverheadScaling() {
    int[] batchSizes = {50, 200, 500, 1000};
    double[] overheadPerChild = new double[batchSizes.length];

    // Warmup with a small batch
    List<String> warmupItems = generateItems(10);
    JobHandle warmupHandle =
        jobService
            .enqueueBatch("warmup-contention")
            .forEach(warmupItems, TimingJob::processBatchItem)
            .submit();
    JobAssertions.assertBatchSucceeded(jobCrudStore, warmupHandle, PERF_TIMEOUT);

    for (int i = 0; i < batchSizes.length; i++) {
      int size = batchSizes[i];
      PerformanceMetricsCollector.reset();
      TimingJob.resetCount();

      List<String> items = generateItems(size);
      log.info("Batch contention: measuring " + size + " children (zero-work)");

      long startMs = System.currentTimeMillis();
      JobHandle batchHandle =
          jobService
              .enqueueBatch("contention-" + size)
              .forEach(items, TimingJob::processBatchItem)
              .submit();
      JobAssertions.assertBatchSucceeded(jobCrudStore, batchHandle, PERF_TIMEOUT);
      long totalMs = System.currentTimeMillis() - startMs;

      overheadPerChild[i] = (double) totalMs / size;

      log.info(
          String.format(
              "Batch contention [%d]: total=%dms, overhead/child=%.2fms",
              size, totalMs, overheadPerChild[i]));

      reportWriter()
          .addReport(
              new PerformanceReport(
                  "batchContention." + size, size, totalMs, (size * 1000.0) / totalMs, 0, 0, 0));
      baseline()
          .assertLatencyWithinTolerance(
              "batchContention." + size + ".overheadPerChildMs", overheadPerChild[i]);
    }

    // Compute scaling ratio: overhead at 1000 vs overhead at 50
    double scalingRatio = overheadPerChild[batchSizes.length - 1] / overheadPerChild[0];
    log.info(
        String.format(
            "Batch contention scaling ratio (1000/50): %.2f (1.0 = constant, >1.0 = contention)",
            scalingRatio));

    baseline().assertLatencyWithinTolerance("batchContention.scalingRatio", scalingRatio);
  }
}
