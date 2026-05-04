package run.ratchet.testsuite.performance;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Benchmarks store SPI operations directly, bypassing the scheduler engine. Measures the raw
 * performance of persistence operations.
 */
class StoreOperationLatencyIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(StoreOperationLatencyIT.class.getName());
  private static final PerformanceBaseline baseline = createBaseline();
  private static final PerformanceReportWriter reportWriter = createReportWriter();

  @Inject private JobStatusStore jobStatusStore;

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

  @AfterEach
  void writeResults() {
    reportWriter.writeClassFragment(getClass().getSimpleName());
    baseline.writeRecordedBaselines();
  }

  @Test
  void saveThenFindById() {
    int warmup = getWarmupCount();
    int measured = getMeasuredCount();

    // Warmup
    for (int i = 0; i < warmup; i++) {
      JobEntity job = createJobEntity("warmup-save-" + i);
      JobEntity saved = jobCrudStore.save(job);
      jobCrudStore.findById(saved.getId());
    }

    // Measured save operations
    long[] saveTimes = new long[measured];
    long[] findTimes = new long[measured];

    for (int i = 0; i < measured; i++) {
      JobEntity job = createJobEntity("measured-save-" + i);
      long start = System.nanoTime();
      JobEntity saved = jobCrudStore.save(job);
      long afterSave = System.nanoTime();
      jobCrudStore.findById(saved.getId());
      long afterFind = System.nanoTime();

      saveTimes[i] = (afterSave - start) / 1_000_000;
      findTimes[i] = (afterFind - afterSave) / 1_000_000;
    }

    long[] savePercentiles = computePercentiles(saveTimes, 0.50, 0.95, 0.99);
    long[] findPercentiles = computePercentiles(findTimes, 0.50, 0.95, 0.99);

    log.info(
        String.format(
            "save(): p50=%dms, p95=%dms, p99=%dms",
            savePercentiles[0], savePercentiles[1], savePercentiles[2]));
    log.info(
        String.format(
            "findById(): p50=%dms, p95=%dms, p99=%dms",
            findPercentiles[0], findPercentiles[1], findPercentiles[2]));

    reportWriter.addReport(
        new PerformanceReport(
            "store.save",
            measured,
            0,
            0,
            savePercentiles[0],
            savePercentiles[1],
            savePercentiles[2]));
    reportWriter.addReport(
        new PerformanceReport(
            "store.findById",
            measured,
            0,
            0,
            findPercentiles[0],
            findPercentiles[1],
            findPercentiles[2]));

    baseline.assertLatencyWithinTolerance("store.save.p99Ms", savePercentiles[2]);
    baseline.assertLatencyWithinTolerance("store.findById.p99Ms", findPercentiles[2]);
  }

  @Test
  void statusTransitionCAS() {
    int measured = getMeasuredCount();
    long[] casTimes = new long[measured];

    for (int i = 0; i < measured; i++) {
      JobEntity job = createJobEntity("cas-" + i);
      JobEntity saved = jobCrudStore.save(job);

      long start = System.nanoTime();
      jobStatusStore.compareAndSwapStatus(
          saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
      long elapsed = System.nanoTime() - start;
      casTimes[i] = elapsed / 1_000_000;
    }

    long[] percentiles = computePercentiles(casTimes, 0.50, 0.95, 0.99);

    log.info(
        String.format(
            "CAS: p50=%dms, p95=%dms, p99=%dms", percentiles[0], percentiles[1], percentiles[2]));

    reportWriter.addReport(
        new PerformanceReport(
            "store.cas", measured, 0, 0, percentiles[0], percentiles[1], percentiles[2]));
    baseline.assertLatencyWithinTolerance("store.cas.p99Ms", percentiles[2]);
  }

  @Test
  void pollingQueryLatency() {
    // Insert 1000 jobs in mixed states
    for (int i = 0; i < 1000; i++) {
      JobEntity job = createJobEntity("poll-" + i);
      if (i % 3 == 0) {
        job.setStatus(JobStatus.RUNNING);
      } else if (i % 3 == 1) {
        job.setStatus(JobStatus.SUCCEEDED);
      }
      // else stays PENDING
      jobCrudStore.save(job);
    }

    int measured = getMeasuredCount();
    long[] pollTimes = new long[measured];
    Instant now = Instant.now();

    for (int i = 0; i < measured; i++) {
      long start = System.nanoTime();
      jobCrudStore.countReadyJobs(now);
      long elapsed = System.nanoTime() - start;
      pollTimes[i] = elapsed / 1_000_000;
    }

    long[] percentiles = computePercentiles(pollTimes, 0.50, 0.95, 0.99);

    log.info(
        String.format(
            "Poll query: p50=%dms, p95=%dms, p99=%dms",
            percentiles[0], percentiles[1], percentiles[2]));

    reportWriter.addReport(
        new PerformanceReport(
            "store.poll", measured, 0, 0, percentiles[0], percentiles[1], percentiles[2]));
    baseline.assertLatencyWithinTolerance("store.poll.p99Ms", percentiles[2]);
  }

  private JobEntity createJobEntity(String suffix) {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setJobType(JobExecutionType.SINGLE);
    job.setTargetClass(TimingJob.class.getName());
    job.setMethodName("execute");
    job.setScheduledTime(Instant.now());
    job.setBusinessKey("perf-store-" + suffix);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload(TimingJob.class.getName(), "execute", "()V", true, List.of()));
    return job;
  }
}
