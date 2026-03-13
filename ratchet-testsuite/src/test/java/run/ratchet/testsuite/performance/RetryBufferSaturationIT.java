package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
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
 * Tests retry buffer backpressure under sustained overload. Burst-enqueues many jobs with sleep to
 * exceed the default thread pool capacity, then measures how many survive vs go to DLQ.
 */
class RetryBufferSaturationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(RetryBufferSaturationIT.class.getName());
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

  @BeforeEach
  void resetCounters() {
    ConfigurableWorkJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
  }

  @Test
  void sustainedOverloadRecovery() {
    int jobCount = 500;
    ConfigurableWorkJob.setSleepMs(200);

    log.info(
        "Buffer saturation: burst-enqueuing "
            + jobCount
            + " jobs with 200ms sleep (far exceeds ~10 thread pool)");

    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(jobCount, ConfigurableWorkJob::execute);
    awaitAllTerminal(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    long succeeded = jobCrudStore.countJobsByStatus(JobStatus.SUCCEEDED);
    long failed = jobCrudStore.countJobsByStatus(JobStatus.FAILED);
    double succeededRate = (double) succeeded / jobCount;

    log.info(
        String.format(
            "Buffer saturation: %d/%d succeeded (%.1f%%), %d failed, recovery=%dms",
            succeeded, jobCount, succeededRate * 100, failed, totalMs));

    assertTrue(
        succeededRate > 0.95,
        String.format(
            "Expected >95%% success rate but got %.1f%% (%d/%d succeeded)",
            succeededRate * 100, succeeded, jobCount));

    reportWriter.addReport(
        new PerformanceReport("bufferSaturation", jobCount, totalMs, succeededRate, 0, 0, failed));
    baseline.assertWithinTolerance("bufferSaturation.succeededRate", succeededRate);
    baseline.assertLatencyWithinTolerance("bufferSaturation.recoveryMs", totalMs);
  }
}
