package run.ratchet.testsuite.performance;

import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.NoOpResilienceStrategy;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Measures the overhead of the retry/failure path compared to a clean-run baseline. The failure
 * path involves logging, {@code @DoNotRetry} checks, CAS retry increment, {@code RetryPolicy}
 * consultation, scheduled_time UPDATE, and event publishing — multiple DB round-trips per failure.
 */
class FailureRateOverheadIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(FailureRateOverheadIT.class.getName());
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
            NoOpResilienceStrategy.class,
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
    ProbabilisticFailingJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
  }

  @Test
  void retryPathOverhead() {
    int count = 100;
    int warmup = getWarmupCount();

    List<JobHandle> warmupHandles = enqueueN(warmup, TimingJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    // Phase 1: Clean baseline (0% failure)
    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    log.info("Failure overhead: Phase 1 — " + count + " clean jobs (0% failure)");
    long baselineStart = System.currentTimeMillis();
    List<JobHandle> baselineHandles = enqueueN(count, TimingJob::execute);
    awaitAllCompleted(baselineHandles, PERF_TIMEOUT);
    long baselineMs = System.currentTimeMillis() - baselineStart;
    double baselineThroughput = (count * 1000.0) / baselineMs;

    // Phase 2: 30% failure with retries
    ProbabilisticFailingJob.reset();
    ProbabilisticFailingJob.setFailureRate(0.30);
    PerformanceMetricsCollector.reset();

    log.info("Failure overhead: Phase 2 — " + count + " jobs (30% failure, maxRetries=3)");
    long failureStart = System.currentTimeMillis();
    List<JobHandle> failureHandles =
        enqueueNWithRetries(count, ProbabilisticFailingJob::execute, 3);
    awaitAllTerminal(failureHandles, PERF_TIMEOUT);
    long failureMs = System.currentTimeMillis() - failureStart;
    double failureThroughput = (count * 1000.0) / failureMs;

    double overheadRatio = baselineThroughput / failureThroughput;

    log.info(
        String.format(
            "Failure overhead: baseline=%.1f jobs/sec, withFailures=%.1f jobs/sec, ratio=%.2f"
                + " (successes=%d, failures=%d)",
            baselineThroughput,
            failureThroughput,
            overheadRatio,
            ProbabilisticFailingJob.getSuccessCount(),
            ProbabilisticFailingJob.getFailureCount()));

    reportWriter.addReport(
        new PerformanceReport(
            "failureOverhead.baseline", count, baselineMs, baselineThroughput, 0, 0, 0));
    reportWriter.addReport(
        new PerformanceReport(
            "failureOverhead.withFailures", count, failureMs, failureThroughput, 0, 0, 0));

    baseline.assertWithinTolerance("failureOverhead.baselineJobsPerSec", baselineThroughput);
    baseline.assertWithinTolerance("failureOverhead.withFailuresJobsPerSec", failureThroughput);
    baseline.assertLatencyWithinTolerance("failureOverhead.ratio", overheadRatio);
  }
}
