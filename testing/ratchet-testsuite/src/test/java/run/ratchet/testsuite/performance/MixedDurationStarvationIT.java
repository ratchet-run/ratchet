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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
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
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceReport;

/**
 * Tests thread pool fairness when fast and slow jobs share the same pool. Measures whether slow
 * jobs (holding permits longer) starve fast jobs by inflating their queue wait time.
 */
class MixedDurationStarvationIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(MixedDurationStarvationIT.class.getName());

  @Deployment
  public static WebArchive createDeployment() {
    return createPerformanceDeployment(ConfigurableWorkJob.class, ProbabilisticFailingJob.class);
  }

  @BeforeEach
  void resetCounters() {
    TimingJob.resetCount();
    ConfigurableWorkJob.reset();
    PerformanceMetricsCollector.reset();
  }

  @Test
  void fastJobBaselineLatency() {
    int warmup = getWarmupCount();

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

    reportWriter()
        .addReport(new PerformanceReport("starvation.fastOnly", count, totalMs, 0, 0, 0, fastP99));
    baseline().assertLatencyWithinTolerance("starvation.fastOnly.p99Ms", fastP99);
  }

  @Test
  void mixedWorkloadStarvation() {
    int warmup = getWarmupCount();

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

    int slowJobsCompleted = ConfigurableWorkJob.getInvocationCount();
    long fastP99 = perfHelper.queryQueueWaitPercentileForClass(TimingJob.class.getName(), 0.99);

    log.info(
        String.format(
            "Mixed workload: %d total jobs in %dms, fast p99 queue wait=%dms",
            fastCount + slowCount, totalMs, fastP99));

    reportWriter()
        .addReport(
            new PerformanceReport(
                "starvation.mixed", fastCount + slowCount, totalMs, 0, 0, 0, fastP99));
    assertEquals(
        slowCount, slowJobsCompleted, "Mixed workload should execute every scheduled slow job");
    baseline().assertLatencyWithinTolerance("starvation.mixed.fastP99Ms", fastP99);
  }
}
