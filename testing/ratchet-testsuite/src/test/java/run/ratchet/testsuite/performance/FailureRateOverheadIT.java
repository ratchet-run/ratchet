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
import run.ratchet.testsuite.app.NoOpResilienceStrategy;
import run.ratchet.testsuite.app.PerformanceMetricsCollector;
import run.ratchet.testsuite.app.ProbabilisticFailingJob;
import run.ratchet.testsuite.app.TimingJob;
import run.ratchet.testsuite.util.PerformanceReport;

/**
 * Measures the overhead of the retry/failure path compared to a clean-run baseline. The failure
 * path involves logging, {@code @DoNotRetry} checks, CAS retry increment, {@code RetryPolicy}
 * consultation, scheduled_time UPDATE, and event publishing — multiple DB round-trips per failure.
 */
class FailureRateOverheadIT extends BasePerformanceIT {

  private static final Logger log = Logger.getLogger(FailureRateOverheadIT.class.getName());

  @Deployment
  public static WebArchive createDeployment() {
    return createPerformanceDeployment(
        ConfigurableWorkJob.class, ProbabilisticFailingJob.class, NoOpResilienceStrategy.class);
  }

  @BeforeEach
  void resetCounters() {
    TimingJob.resetCount();
    ProbabilisticFailingJob.reset();
    PerformanceMetricsCollector.reset();
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
    long terminalSuccesses = countHandlesWithStatus(failureHandles, JobStatus.SUCCEEDED);
    long terminalFailures = countHandlesWithStatus(failureHandles, JobStatus.FAILED);
    int invocationSuccesses = ProbabilisticFailingJob.getSuccessCount();
    int invocationFailures = ProbabilisticFailingJob.getFailureCount();
    int totalInvocations = invocationSuccesses + invocationFailures;
    double actualFailureRate = (double) invocationFailures / totalInvocations;

    log.info(
        String.format(
            "Failure overhead: baseline=%.1f jobs/sec, withFailures=%.1f jobs/sec, ratio=%.2f"
                + " (successes=%d, failures=%d, terminalFailures=%d)",
            baselineThroughput,
            failureThroughput,
            overheadRatio,
            invocationSuccesses,
            invocationFailures,
            terminalFailures));

    reportWriter()
        .addReport(
            new PerformanceReport(
                "failureOverhead.baseline", count, baselineMs, baselineThroughput, 0, 0, 0));
    reportWriter()
        .addReport(
            new PerformanceReport(
                "failureOverhead.withFailures", count, failureMs, failureThroughput, 0, 0, 0));

    baseline().assertWithinTolerance("failureOverhead.baselineJobsPerSec", baselineThroughput);
    baseline().assertWithinTolerance("failureOverhead.withFailuresJobsPerSec", failureThroughput);
    baseline().assertLatencyWithinTolerance("failureOverhead.ratio", overheadRatio);
    assertEquals(
        count,
        terminalSuccesses + terminalFailures,
        "Failure phase should account for every submitted job");
    assertTrue(
        terminalFailures <= maxTerminalFailureCount(count),
        "Retry phase left too many terminal failures: "
            + terminalFailures
            + " of "
            + count
            + " jobs");
    assertTrue(
        totalInvocations >= count,
        "Retry phase should have at least one invocation per submitted job but had "
            + totalInvocations);
    assertTrue(invocationFailures > 0, "Expected the configured failure path to be exercised");
    assertTrue(
        actualFailureRate >= minObservedFailureRate()
            && actualFailureRate <= maxObservedFailureRate(),
        String.format(
            "Observed failure rate %.2f outside expected range [%.2f, %.2f]",
            actualFailureRate, minObservedFailureRate(), maxObservedFailureRate()));
  }

  private static long maxTerminalFailureCount(int submittedJobs) {
    return Long.getLong("perf.failure.maxTerminalFailures", Math.max(1, submittedJobs / 10));
  }

  private static double minObservedFailureRate() {
    return Double.parseDouble(System.getProperty("perf.failure.minObservedRate", "0.15"));
  }

  private static double maxObservedFailureRate() {
    return Double.parseDouble(System.getProperty("perf.failure.maxObservedRate", "0.45"));
  }
}
