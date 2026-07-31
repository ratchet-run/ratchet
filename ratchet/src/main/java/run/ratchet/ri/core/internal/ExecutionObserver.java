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
package run.ratchet.ri.core.internal;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.spi.JobAuditStore;

/**
 * Bundles metrics, event publishing, and execution history to reduce {@link JobTask}'s dependency
 * count.
 *
 * @see JobTask
 */
public class ExecutionObserver {

  private static final Logger log = Logger.getLogger(ExecutionObserver.class);

  private final MetricsCollector metricsCollector;
  private final TracingCollector tracingCollector;
  private final InternalEventPublisher eventPublisher;
  private final JobAuditStore executionStore;
  private final ExecutorProvider executorProvider;
  private final Runnable delayedJobReadyCallback;

  protected ExecutionObserver() {
    this.metricsCollector = null;
    this.tracingCollector = null;
    this.eventPublisher = null;
    this.executionStore = null;
    this.executorProvider = null;
    this.delayedJobReadyCallback = null;
  }

  public ExecutionObserver(
      MetricsCollector metricsCollector,
      TracingCollector tracingCollector,
      InternalEventPublisher eventPublisher,
      JobAuditStore executionStore,
      ExecutorProvider executorProvider,
      Runnable delayedJobReadyCallback) {
    this.metricsCollector = metricsCollector;
    this.tracingCollector = tracingCollector;
    this.eventPublisher = eventPublisher;
    this.executionStore = executionStore;
    this.executorProvider = executorProvider;
    this.delayedJobReadyCallback = delayedJobReadyCallback;
  }

  /** Creates the default observer without an optional delayed-job callback. */
  public ExecutionObserver(
      MetricsCollector metricsCollector,
      TracingCollector tracingCollector,
      InternalEventPublisher eventPublisher,
      JobAuditStore executionStore,
      ExecutorProvider executorProvider) {
    this(
        metricsCollector, tracingCollector, eventPublisher, executionStore, executorProvider, null);
  }

  public void recordJobStart(JobEntity job) {
    recordQuietly(
        "jobStarted",
        () -> metricsCollector.jobStarted(job.getId(), job.getPublicJobType(), job.getPriority()));
  }

  public void recordJobSuccess(JobEntity job, long executionTimeMs) {
    recordQuietly(
        "jobCompleted",
        () -> metricsCollector.jobCompleted(job.getId(), job.getPublicJobType(), executionTimeMs));
  }

  public void recordJobFailure(JobEntity job, Throwable ex, int attempt) {
    recordQuietly(
        "jobFailed",
        () -> metricsCollector.jobFailed(job.getId(), job.getPublicJobType(), ex, attempt));
  }

  public void recordCallbackFailure(JobEntity job, Throwable ex, int attempt) {
    recordQuietly(
        "callbackFailed",
        () -> metricsCollector.callbackFailed(job.getId(), job.getPublicJobType(), ex, attempt));
  }

  public void recordSuccessFinalizationRetry(JobEntity job) {
    recordQuietly(
        "successFinalizationRetried",
        () -> metricsCollector.successFinalizationRetried(job.getId(), job.getPublicJobType()));
  }

  public void recordSuccessFinalizationMinimal(JobEntity job) {
    recordQuietly(
        "successFinalizationMinimal",
        () -> metricsCollector.successFinalizationMinimal(job.getId(), job.getPublicJobType()));
  }

  public void recordSuccessFinalizationStuck(JobEntity job) {
    recordQuietly(
        "successFinalizationStuck",
        () -> metricsCollector.successFinalizationStuck(job.getId(), job.getPublicJobType()));
  }

  public void recordJobCancellation(JobEntity job) {
    // The public MetricsCollector SPI has no cancellation callback.
  }

  public void recordEnvelopeVersionSkew(UUID jobId, int version, int maxSupportedVersion) {
    recordQuietly(
        "encryptionEnvelopeVersionSkew",
        () -> metricsCollector.encryptionEnvelopeVersionSkew(jobId, version, maxSupportedVersion));
  }

  /**
   * Runs a metrics callback, swallowing and logging any failure. The {@link MetricsCollector} SPI
   * is integrator-supplied observability; a throwing collector must never change a job's outcome.
   */
  private void recordQuietly(String metric, Runnable action) {
    try {
      action.run();
    } catch (Throwable t) {
      log.warnf(t, "MetricsCollector.%s threw; metrics for this event are dropped", metric);
    }
  }

  public void publishEvent(Object event) {
    eventPublisher.publish(event);
  }

  public void scheduleDelayedJobReadyCallback(long delayMs) {
    if (delayedJobReadyCallback != null) {
      executorProvider
          .getScheduledExecutor()
          .schedule(delayedJobReadyCallback, delayMs, TimeUnit.MILLISECONDS);
    }
  }

  public JobExecutionEntity startExecution(UUID jobId, int attemptNumber, String nodeId) {
    JobExecutionEntity execution = JobExecutionEntity.start(jobId, attemptNumber, nodeId);
    if (executionStore == null) {
      // No JobAuditStore capability: execution-history recording is disabled. Return the transient
      // entity so callers still have a usable handle for the in-flight attempt.
      return execution;
    }
    return executionStore.saveExecution(execution);
  }

  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (executionStore == null) {
      return execution;
    }
    return executionStore.saveExecution(execution);
  }

  /**
   * Starts a tracing scope for one job execution attempt. The caller must close the returned scope
   * in a {@code finally} block regardless of outcome.
   *
   * <p>The {@code parentContext} passed to {@link TracingCollector#jobExecutionStarted} is the
   * carrier map captured at enqueue time via {@link TracingCollector#captureCurrentContext()}. When
   * no tracing is active, or no context was captured, the map is empty and the implementation
   * creates a root span.
   */
  public TracingCollector.ExecutionScope startExecutionScope(JobEntity job) {
    if (tracingCollector == null) {
      return TracingCollector.NoOpExecutionScope.INSTANCE;
    }
    try {
      Map<String, String> parentContext = job.getTraceContext();
      TracingCollector.ExecutionScope scope =
          tracingCollector.jobExecutionStarted(
              job.getId(),
              job.getPublicJobType(),
              job.getPriority(),
              parentContext != null ? parentContext : Map.of(),
              signalTraceAttributes(job));
      return scope == null
          ? TracingCollector.NoOpExecutionScope.INSTANCE
          : new GuardedExecutionScope(scope);
    } catch (Throwable t) {
      log.warnf(
          t,
          "TracingCollector failed to start an execution scope for job %s; tracing is disabled for"
              + " this attempt",
          job.getId());
      return TracingCollector.NoOpExecutionScope.INSTANCE;
    }
  }

  private Map<String, String> signalTraceAttributes(JobEntity job) {
    if (job.getSignalKey() == null) {
      return Map.of();
    }
    Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("ratchet.signal.key", job.getSignalKey());
    if (job.getSignalOutcome() != null) {
      attributes.put("ratchet.signal.outcome", job.getSignalOutcome());
    }
    if (job.getSignalDeliveredBy() != null) {
      attributes.put("ratchet.signal.delivered_by.present", "true");
    }
    if (job.getCreatedAt() != null && job.getSignalDeliveredAt() != null) {
      long waitMs = Duration.between(job.getCreatedAt(), job.getSignalDeliveredAt()).toMillis();
      attributes.put("ratchet.signal.wait_ms", Long.toString(Math.max(0L, waitMs)));
    }
    return Map.copyOf(attributes);
  }

  /**
   * Wraps a {@link TracingCollector.ExecutionScope} so an exception from the integrator's tracer
   * cannot escape onto the job hot path. Tracing is observability: a throwing scope must never
   * change a job's outcome.
   */
  private static final class GuardedExecutionScope implements TracingCollector.ExecutionScope {
    private final TracingCollector.ExecutionScope delegate;

    GuardedExecutionScope(TracingCollector.ExecutionScope delegate) {
      this.delegate = delegate;
    }

    @Override
    public void success(long executionTimeMs) {
      try {
        delegate.success(executionTimeMs);
      } catch (Throwable t) {
        log.warnf(t, "TracingCollector scope.success threw; continuing");
      }
    }

    @Override
    public void failure(Throwable cause, int attempt) {
      try {
        delegate.failure(cause, attempt);
      } catch (Throwable t) {
        log.warnf(t, "TracingCollector scope.failure threw; continuing");
      }
    }

    @Override
    public void close() {
      try {
        delegate.close();
      } catch (Throwable t) {
        log.warnf(t, "TracingCollector scope.close threw; continuing");
      }
    }
  }
}
