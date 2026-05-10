package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Measures recovery from executor backlog pressure. Burst-enqueues more sleeping jobs than the
 * default worker pool can run at once, then verifies they eventually drain successfully.
 */
class ExecutorBacklogRecoveryIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(ExecutorBacklogRecoveryIT.class.getName());

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
    ConfigurableWorkJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @Test
  void sustainedOverloadRecovery() {
    int jobCount = 200;
    ConfigurableWorkJob.setSleepMs(100);

    log.info(
        "Executor backlog: burst-enqueuing "
            + jobCount
            + " jobs with 100ms sleep (exceeds ~10 thread pool)");

    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(jobCount, ConfigurableWorkJob::execute);
    awaitAllTerminal(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    long succeeded = jobCrudStore.countJobsByStatus(JobStatus.SUCCEEDED);
    long failed = jobCrudStore.countJobsByStatus(JobStatus.FAILED);
    double succeededRate = (double) succeeded / jobCount;

    log.info(
        String.format(
            "Executor backlog: %d/%d succeeded (%.1f%%), %d failed, recovery=%dms",
            succeeded, jobCount, succeededRate * 100, failed, totalMs));

    assertTrue(
        succeededRate > 0.95,
        String.format(
            "Expected >95%% success rate but got %.1f%% (%d/%d succeeded)",
            succeededRate * 100, succeeded, jobCount));

    reportWriter()
        .addReport(
            new PerformanceReport(
                "executorBacklog", jobCount, totalMs, succeededRate, 0, 0, failed));
    baseline().assertWithinTolerance("executorBacklog.succeededRate", succeededRate);
    baseline().assertLatencyWithinTolerance("executorBacklog.recoveryMs", totalMs);
  }
}
