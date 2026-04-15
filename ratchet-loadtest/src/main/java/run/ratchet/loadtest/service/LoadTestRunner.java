package run.ratchet.loadtest.service;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.loadtest.api.EnqueueJobRequest;
import run.ratchet.loadtest.api.JobEnqueuedResponse;
import run.ratchet.loadtest.api.StartRunRequest;
import run.ratchet.loadtest.metrics.PrometheusRegistryProducer;
import run.ratchet.loadtest.workload.LoadTestWorkloadExecutor;
import run.ratchet.loadtest.workload.WorkloadType;
import run.ratchet.spi.NodeIdentityProvider;
import io.micrometer.core.instrument.Counter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@ApplicationScoped
public class LoadTestRunner {

  private static final int MAX_JOBS_PER_REQUEST =
      Integer.getInteger("ratchet.loadtest.max-jobs-per-request", 1_000_000);

  @Inject JobSchedulerService scheduler;
  @Inject LoadTestWorkloadExecutor workloadExecutor;
  @Inject PrometheusRegistryProducer prometheusRegistry;
  @Inject RunRegistry runRegistry;
  @Inject NodeIdentityProvider nodeIdentityProvider;

  public RunMetadata start(StartRunRequest request) {
    StartRunRequest req = request == null ? new StartRunRequest() : request;
    int jobs = validateJobs(req.jobs);
    JobConfig config =
        new JobConfig(
            WorkloadType.parse(req.workload),
            Math.max(0, req.sleepMs),
            Math.max(0, req.sleepJitterMs),
            Math.max(0.0, Math.min(1.0, req.sleepSpikeRate)),
            Math.max(0, req.sleepSpikeMs),
            Math.max(0.0, Math.min(1.0, req.failureRate)),
            payload(Math.max(0, req.payloadBytes)),
            Math.max(0, req.maxRetries),
            parsePriority(req.priority),
            Duration.ofSeconds(Math.max(1, req.timeoutSeconds)));

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
            WorkloadType.parse(req.workload),
            Math.max(0, req.sleepMs),
            Math.max(0, req.sleepJitterMs),
            Math.max(0.0, Math.min(1.0, req.sleepSpikeRate)),
            Math.max(0, req.sleepSpikeMs),
            Math.max(0.0, Math.min(1.0, req.failureRate)),
            payload(Math.max(0, req.payloadBytes)),
            Math.max(0, req.maxRetries),
            parsePriority(req.priority),
            Duration.ofSeconds(Math.max(1, req.timeoutSeconds)));
    String runId =
        req.runId == null || req.runId.isBlank() ? UUID.randomUUID().toString() : req.runId;
    String acceptedNodeId = nodeIdentityProvider.getNodeId();
    JobHandle handle = submitJob(runId, req.sequence, config, acceptedNodeId);
    recordSubmitted(config.workload().name(), "http", 1);
    return new JobEnqueuedResponse(
        runId, handle.id(), req.sequence, config.workload().name(), acceptedNodeId, Instant.now());
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
    Counter.builder("ratchet.loadtest.jobs.submitted")
        .description("Load-test jobs submitted")
        .tag("workload", workloadName)
        .tag("source", source)
        .register(prometheusRegistry.meterRegistry())
        .increment(jobs);
  }

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
