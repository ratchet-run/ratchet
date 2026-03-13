package run.ratchet.testsuite.performance;

import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.PerformanceMetricsCollector.PerformanceSnapshot;
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

/** Measures raw job processing throughput with no-op and light workload jobs. */
class ThroughputIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ThroughputIT.class.getName());
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
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
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
  void noOpThroughput() {
    int warmup = getWarmupCount();
    int measured = getMeasuredCount();

    // Warmup phase
    log.info("Warmup: enqueuing " + warmup + " no-op jobs");
    List<JobHandle> warmupHandles = enqueueN(warmup, TimingJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    // Reset metrics for measurement
    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    // Measured phase
    log.info("Measured: enqueuing " + measured + " no-op jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();
    double throughput = (measured * 1000.0) / totalMs;

    log.info(
        String.format(
            "No-op throughput: %.1f jobs/sec (%d jobs in %d ms)", throughput, measured, totalMs));

    PerformanceReport report =
        new PerformanceReport(
            "throughput.noOp",
            measured,
            totalMs,
            throughput,
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter.addReport(report);
    baseline.assertWithinTolerance("throughput.noOp.jobsPerSec", throughput);
  }

  @Test
  void lightWorkloadThroughput() {
    int warmup = getWarmupCount();
    int measured = getMeasuredCount();

    ConfigurableWorkJob.setSleepMs(5);

    // Warmup phase
    List<JobHandle> warmupHandles = enqueueN(warmup, ConfigurableWorkJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    // Reset metrics for measurement
    PerformanceMetricsCollector.reset();
    ConfigurableWorkJob.reset();
    ConfigurableWorkJob.setSleepMs(5);

    // Measured phase
    log.info("Measured: enqueuing " + measured + " 5ms-work jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, ConfigurableWorkJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();
    double throughput = (measured * 1000.0) / totalMs;

    log.info(
        String.format(
            "Light workload throughput: %.1f jobs/sec (%d jobs in %d ms)",
            throughput, measured, totalMs));

    PerformanceReport report =
        new PerformanceReport(
            "throughput.lightWork",
            measured,
            totalMs,
            throughput,
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter.addReport(report);
    baseline.assertWithinTolerance("throughput.lightWork.jobsPerSec", throughput);
  }
}
