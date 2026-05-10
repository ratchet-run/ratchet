package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

/** Measures raw job processing throughput with no-op and light workload jobs. */
class ThroughputIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ThroughputIT.class.getName());

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

  @Test
  void noOpThroughput() {
    int warmup = getWarmupCount();
    int measured = getMeasuredCount();

    log.info("Warmup: enqueuing " + warmup + " no-op jobs");
    List<JobHandle> warmupHandles = enqueueN(warmup, TimingJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    log.info("Measured: enqueuing " + measured + " no-op jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    assertEquals(
        measured,
        TimingJob.getInvocationCount(),
        "Expected no-op job invocation count to match completed jobs");
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
    reportWriter().addReport(report);
    baseline().assertWithinTolerance("throughput.noOp.jobsPerSec", throughput);
  }

  @Test
  void lightWorkloadThroughput() {
    int warmup = getWarmupCount();
    int measured = getMeasuredCount();

    ConfigurableWorkJob.setSleepMs(5);

    List<JobHandle> warmupHandles = enqueueN(warmup, ConfigurableWorkJob::execute);
    awaitAllCompleted(warmupHandles, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    ConfigurableWorkJob.reset();
    ConfigurableWorkJob.setSleepMs(5);

    log.info("Measured: enqueuing " + measured + " 5ms-work jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(measured, ConfigurableWorkJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    assertEquals(
        measured,
        ConfigurableWorkJob.getInvocationCount(),
        "Expected light-work job invocation count to match completed jobs");
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
    reportWriter().addReport(report);
    baseline().assertWithinTolerance("throughput.lightWork.jobsPerSec", throughput);
  }
}
