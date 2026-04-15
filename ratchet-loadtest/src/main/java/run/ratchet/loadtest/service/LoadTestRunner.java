package run.ratchet.loadtest.service;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.loadtest.api.StartRunRequest;
import run.ratchet.loadtest.metrics.PrometheusRegistryProducer;
import run.ratchet.loadtest.workload.LoadTestWorkloadExecutor;
import run.ratchet.loadtest.workload.WorkloadType;
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

  public RunMetadata start(StartRunRequest request) {
    StartRunRequest req = request == null ? new StartRunRequest() : request;
    WorkloadType workload = WorkloadType.parse(req.workload);
    int jobs = validateJobs(req.jobs);
    long sleepMs = Math.max(0, req.sleepMs);
    long sleepJitterMs = Math.max(0, req.sleepJitterMs);
    double sleepSpikeRate = Math.max(0.0, Math.min(1.0, req.sleepSpikeRate));
    long sleepSpikeMs = Math.max(0, req.sleepSpikeMs);
    double failureRate = Math.max(0.0, Math.min(1.0, req.failureRate));
    int payloadBytes = Math.max(0, req.payloadBytes);
    int maxRetries = Math.max(0, req.maxRetries);
    JobPriority priority = parsePriority(req.priority);
    Duration timeout = Duration.ofSeconds(Math.max(1, req.timeoutSeconds));

    String runId = UUID.randomUUID().toString();
    String payload = payload(payloadBytes);
    String runTag = Tags.run(runId);
    String workloadName = workload.name();
    String sleepMsValue = Long.toString(sleepMs);
    String sleepJitterMsValue = Long.toString(sleepJitterMs);
    String sleepSpikeRateValue = Double.toString(sleepSpikeRate);
    String sleepSpikeMsValue = Long.toString(sleepSpikeMs);
    String failureRateValue = Double.toString(failureRate);

    for (int i = 0; i < jobs; i++) {
      String sequence = Integer.toString(i);
      scheduler
          .enqueue(
              () ->
                  workloadExecutor.execute(
                      runId,
                      workloadName,
                      sequence,
                      sleepMsValue,
                      sleepJitterMsValue,
                      sleepSpikeRateValue,
                      sleepSpikeMsValue,
                      failureRateValue,
                      payload))
          .withTags(Tags.LOADTEST, runTag, Tags.workload(workload))
          .withPriority(priority)
          .withMaxRetries(maxRetries)
          .withTimeout(timeout)
          .submit();
    }
    Counter.builder("ratchet.loadtest.jobs.submitted")
        .description("Load-test jobs submitted")
        .tag("workload", workloadName)
        .register(prometheusRegistry.meterRegistry())
        .increment(jobs);

    RunMetadata metadata = new RunMetadata(runId, workload.name(), jobs, Instant.now());
    runRegistry.put(metadata);
    return metadata;
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
}
