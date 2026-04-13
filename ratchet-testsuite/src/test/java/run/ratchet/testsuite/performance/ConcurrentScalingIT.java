package run.ratchet.testsuite.performance;

import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
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
    ConfigurableWorkJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
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
    reportWriter.addReport(report);
    baseline.assertWithinTolerance("scaling.noOp.burst.jobsPerSec", throughput);
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
    reportWriter.addReport(report);
    baseline.assertWithinTolerance("scaling.work10ms.burst.jobsPerSec", throughput);
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
      reportWriter.addReport(report);
      baseline.assertWithinTolerance("scaling.incremental." + load + ".jobsPerSec", throughput);
    }
  }
}
