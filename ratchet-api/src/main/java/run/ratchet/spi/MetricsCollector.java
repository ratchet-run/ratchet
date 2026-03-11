package run.ratchet.spi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Collects metrics about job execution for monitoring and alerting. */
public interface MetricsCollector {

  void jobStarted(long jobId, JobType type, JobPriority priority);

  void jobCompleted(long jobId, JobType type, long executionTimeMs);

  void jobFailed(long jobId, JobType type, Throwable cause, int attempt);
}
