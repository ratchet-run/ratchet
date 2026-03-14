package run.ratchet.testsuite.performance;

import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
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

/**
 * Tests thread pool fairness when fast and slow jobs share the same pool. Measures whether slow
 * jobs (holding permits longer) starve fast jobs by inflating their queue wait time.
 */
class MixedDurationStarvationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(MixedDurationStarvationIT.class.getName());
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
  void fastJobBaselineLatency() {
    int warmup = getWarmupCount();

    // Warmup
    List<JobHandle> warmupHandles = enqueueN(warmup, TimingJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    // Measured: 100 fast jobs only
    int count = 100;
    log.info("Fast-only baseline: enqueuing " + count + " no-op jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(count, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    // Query queue_wait_ms for fast jobs via store-specific API
    long fastP99 = perfHelper.queryQueueWaitPercentileForClass(TimingJob.class.getName(), 0.99);

    log.info(
        String.format(
            "Fast-only baseline: %d jobs in %dms, fast p99 queue wait=%dms",
            count, totalMs, fastP99));

    reportWriter.addReport(
        new PerformanceReport("starvation.fastOnly", count, totalMs, 0, 0, 0, fastP99));
    baseline.assertLatencyWithinTolerance("starvation.fastOnly.p99Ms", fastP99);
  }

  @Test
  void mixedWorkloadStarvation() {
    int warmup = getWarmupCount();

    // Warmup
    List<JobHandle> warmupHandles = enqueueN(warmup, TimingJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();
    ConfigurableWorkJob.reset();
    ConfigurableWorkJob.setSleepMs(500);

    // Measured: interleave 80 fast + 20 slow jobs
    int fastCount = 80;
    int slowCount = 20;
    log.info("Mixed workload: " + fastCount + " fast + " + slowCount + " slow (500ms) jobs");

    long startMs = System.currentTimeMillis();
    List<JobHandle> allHandles = new ArrayList<>(fastCount + slowCount);
    for (int i = 0; i < fastCount + slowCount; i++) {
      if (i % 5 == 0) {
        // Every 5th job is slow
        allHandles.add(jobService.enqueueNow(ConfigurableWorkJob::execute));
      } else {
        allHandles.add(jobService.enqueueNow(TimingJob::execute));
      }
    }
    awaitAllCompleted(allHandles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    long fastP99 = perfHelper.queryQueueWaitPercentileForClass(TimingJob.class.getName(), 0.99);

    log.info(
        String.format(
            "Mixed workload: %d total jobs in %dms, fast p99 queue wait=%dms",
            fastCount + slowCount, totalMs, fastP99));

    reportWriter.addReport(
        new PerformanceReport(
            "starvation.mixed", fastCount + slowCount, totalMs, 0, 0, 0, fastP99));
    baseline.assertLatencyWithinTolerance("starvation.mixed.fastP99Ms", fastP99);
  }
}
