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
package run.ratchet.loadtest.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.loadtest.api.EnqueueJobRequest;
import run.ratchet.loadtest.api.JobEnqueuedResponse;
import run.ratchet.loadtest.api.StartRunRequest;
import run.ratchet.loadtest.workload.LoadTestWorkloadExecutor;
import run.ratchet.loadtest.workload.WorkloadType;
import run.ratchet.spi.NodeIdentityProvider;

@ApplicationScoped
public class LoadTestRunner {

  private static final int MAX_JOBS_PER_REQUEST =
      Integer.getInteger("ratchet.loadtest.max-jobs-per-request", 1_000_000);

  @Inject JobSchedulerService scheduler;
  @Inject LoadTestWorkloadExecutor workloadExecutor;
  @Inject MeterRegistry registry;
  @Inject RunRegistry runRegistry;
  @Inject NodeIdentityProvider nodeIdentityProvider;

  // Counters are cached per (workload, source) so we register each combination at most once.
  // PrometheusMeterRegistry rejects duplicate registration; even idempotent registries pay an
  // O(n) lookup on every increment, which inflates the harness's own measured overhead.
  private final ConcurrentMap<String, Counter> submittedCounters = new ConcurrentHashMap<>();

  private static int validateJobs(int jobs) {
    if (jobs <= 0) {
      throw new IllegalArgumentException("jobs must be greater than zero");
    }
    if (jobs > MAX_JOBS_PER_REQUEST) {
      throw new IllegalArgumentException("jobs exceeds max allowed value " + MAX_JOBS_PER_REQUEST);
    }
    return jobs;
  }

  private static JobPriority parsePriority(String raw) {
    if (raw == null || raw.isBlank()) {
      return JobPriority.NORMAL;
    }
    return JobPriority.valueOf(raw.trim().replace('-', '_').toUpperCase(Locale.ROOT));
  }

  private static String payload(int bytes) {
    if (bytes <= 0) {
      return "";
    }
    return "x".repeat(bytes);
  }

  public RunMetadata start(StartRunRequest request) {
    StartRunRequest req = request == null ? new StartRunRequest() : request;
    int jobs = validateJobs(req.getJobs());
    JobConfig config =
        new JobConfig(
            WorkloadType.parse(req.getWorkload()),
            Math.max(0, req.getSleepMs()),
            Math.max(0, req.getSleepJitterMs()),
            Math.max(0.0, Math.min(1.0, req.getSleepSpikeRate())),
            Math.max(0, req.getSleepSpikeMs()),
            Math.max(0.0, Math.min(1.0, req.getFailureRate())),
            payload(Math.max(0, req.getPayloadBytes())),
            Math.max(0, req.getMaxRetries()),
            parsePriority(req.getPriority()),
            Duration.ofSeconds(Math.max(1, req.getTimeoutSeconds())));

    String runId = UUID.randomUUID().toString();
    String acceptedNodeId = nodeIdentityProvider.getNodeId();

    for (int i = 0; i < jobs; i++) {
      submitJob(runId, i, config, acceptedNodeId);
    }
    recordSubmitted(config.workload().name(), "server-batch", jobs);

    RunMetadata metadata = new RunMetadata(runId, config.workload().name(), jobs, Instant.now());
    runRegistry.put(metadata);
    return metadata;
  }

  public JobEnqueuedResponse enqueue(EnqueueJobRequest request) {
    EnqueueJobRequest req = request == null ? new EnqueueJobRequest() : request;
    JobConfig config =
        new JobConfig(
            WorkloadType.parse(req.getWorkload()),
            Math.max(0, req.getSleepMs()),
            Math.max(0, req.getSleepJitterMs()),
            Math.max(0.0, Math.min(1.0, req.getSleepSpikeRate())),
            Math.max(0, req.getSleepSpikeMs()),
            Math.max(0.0, Math.min(1.0, req.getFailureRate())),
            payload(Math.max(0, req.getPayloadBytes())),
            Math.max(0, req.getMaxRetries()),
            parsePriority(req.getPriority()),
            Duration.ofSeconds(Math.max(1, req.getTimeoutSeconds())));
    String runId =
        req.getRunId() == null || req.getRunId().isBlank()
            ? UUID.randomUUID().toString()
            : req.getRunId();
    String acceptedNodeId = nodeIdentityProvider.getNodeId();
    JobHandle handle = submitJob(runId, req.getSequence(), config, acceptedNodeId);
    recordSubmitted(config.workload().name(), "http", 1);
    return new JobEnqueuedResponse(
        runId,
        handle.id(),
        req.getSequence(),
        config.workload().name(),
        acceptedNodeId,
        Instant.now());
  }

  private JobHandle submitJob(String runId, int sequence, JobConfig config, String acceptedNodeId) {
    String workloadName = config.workload().name();
    String sequenceValue = Integer.toString(sequence);
    String sleepMsValue = Long.toString(config.sleepMs());
    String sleepJitterMsValue = Long.toString(config.sleepJitterMs());
    String sleepSpikeRateValue = Double.toString(config.sleepSpikeRate());
    String sleepSpikeMsValue = Long.toString(config.sleepSpikeMs());
    String failureRateValue = Double.toString(config.failureRate());
    String payload = config.payload();
    return scheduler
        .enqueue(
            () ->
                workloadExecutor.execute(
                    runId,
                    workloadName,
                    sequenceValue,
                    sleepMsValue,
                    sleepJitterMsValue,
                    sleepSpikeRateValue,
                    sleepSpikeMsValue,
                    failureRateValue,
                    payload))
        .withTags(
            Tags.LOADTEST,
            Tags.run(runId),
            Tags.workload(config.workload()),
            Tags.enqueueNode(acceptedNodeId))
        .withParam(Tags.PARAM_ENQUEUE_NODE, acceptedNodeId)
        .withPriority(config.priority())
        .withMaxRetries(config.maxRetries())
        .withTimeout(config.timeout())
        .submit();
  }

  private void recordSubmitted(String workloadName, String source, int jobs) {
    submittedCounters
        .computeIfAbsent(
            workloadName + '|' + source,
            key ->
                Counter.builder("ratchet.loadtest.jobs.submitted")
                    .description("Load-test jobs submitted")
                    .tag("workload", workloadName)
                    .tag("source", source)
                    .register(registry))
        .increment(jobs);
  }

  private record JobConfig(
      WorkloadType workload,
      long sleepMs,
      long sleepJitterMs,
      double sleepSpikeRate,
      long sleepSpikeMs,
      double failureRate,
      String payload,
      int maxRetries,
      JobPriority priority,
      Duration timeout) {}
}
