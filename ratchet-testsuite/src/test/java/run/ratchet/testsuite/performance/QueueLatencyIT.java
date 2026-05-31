/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.testsuite.performance;

import java.util.List;
import java.util.logging.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.PerformanceMetricsCollector.PerformanceSnapshot;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceBaseline;
import run.ratchet.testsuite.util.PerformanceReport;
import run.ratchet.testsuite.util.PerformanceReportWriter;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Measures queue wait and execution latency percentiles under varying load conditions. */
class QueueLatencyIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(QueueLatencyIT.class.getName());

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

  @Test
  void queueWaitUnderLightLoad() {
    int count = 100;

    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    log.info("Measuring queue wait under light load: " + count + " jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(count, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    long[] percentiles = queryQueueWaitPercentiles(0.50, 0.95, 0.99);
    PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();

    log.info(
        String.format(
            "Queue wait (light): p50=%dms, p95=%dms, p99=%dms",
            percentiles[0], percentiles[1], percentiles[2]));

    PerformanceReport report =
        new PerformanceReport(
            "latency.queueWait.light",
            count,
            totalMs,
            (count * 1000.0) / totalMs,
            percentiles[0],
            percentiles[1],
            percentiles[2]);
    reportWriter().addReport(report);

    baseline().assertLatencyWithinTolerance("latency.queueWait.light.p99Ms", percentiles[2]);
  }

  @Test
  void queueWaitUnderHeavyLoad() {
    int count = getMeasuredCount();

    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    log.info("Measuring queue wait under heavy load: " + count + " jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(count, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    long[] percentiles = queryQueueWaitPercentiles(0.50, 0.95, 0.99);

    log.info(
        String.format(
            "Queue wait (heavy): p50=%dms, p95=%dms, p99=%dms",
            percentiles[0], percentiles[1], percentiles[2]));

    PerformanceReport report =
        new PerformanceReport(
            "latency.queueWait.heavy",
            count,
            totalMs,
            (count * 1000.0) / totalMs,
            percentiles[0],
            percentiles[1],
            percentiles[2]);
    reportWriter().addReport(report);

    baseline().assertLatencyWithinTolerance("latency.queueWait.heavy.p99Ms", percentiles[2]);
  }

  @Test
  void executionLatencyDistribution() {
    int count = getMeasuredCount();

    List<JobHandle> warmup = enqueueN(getWarmupCount(), TimingJob::execute);
    awaitAllCompleted(warmup, PERF_TIMEOUT);

    PerformanceMetricsCollector.reset();
    TimingJob.resetCount();

    log.info("Measuring execution latency distribution: " + count + " jobs");
    long startMs = System.currentTimeMillis();
    List<JobHandle> handles = enqueueN(count, TimingJob::execute);
    awaitAllCompleted(handles, PERF_TIMEOUT);
    long totalMs = System.currentTimeMillis() - startMs;

    PerformanceSnapshot snap = PerformanceMetricsCollector.snapshot();

    log.info(
        String.format(
            "Execution latency: p50=%dms, p95=%dms, p99=%dms",
            snap.p50Ms(), snap.p95Ms(), snap.p99Ms()));

    PerformanceReport report =
        new PerformanceReport(
            "latency.execution",
            count,
            totalMs,
            snap.throughputJobsPerSec(),
            snap.p50Ms(),
            snap.p95Ms(),
            snap.p99Ms());
    reportWriter().addReport(report);

    baseline().assertLatencyWithinTolerance("latency.execution.p99Ms", snap.p99Ms());
  }
}
