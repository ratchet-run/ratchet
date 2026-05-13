package run.ratchet.api;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import run.ratchet.api.exception.SignalTimeoutException;

/**
 * Fluent builder for configuring and submitting individual jobs. Supports chaining, conditional
 * branching, retry policies, and lifecycle callbacks.
 *
 * <pre>{@code
 * JobHandle handle = schedulerService.enqueue(() -> processOrder(orderId))
 *     .withPriority(JobPriority.HIGH)
 *     .withTimeout(Duration.ofMinutes(5))
 *     .withMaxRetries(3)
 *     .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(10))
 *     .withTags("order-processing", "customer-123")
 *     .onSuccess(ctx -> log.info("Order {} processed", orderId))
 *     .onFailure((ctx, error) -> alertService.sendAlert(error))
 *     .submit();
 * }</pre>
 *
 * <p>Instances are obtained through {@link JobSubmitter} methods.
 *
 * @see JobSubmitter
 * @see JobOptions
 * @see JobHandle
 */
public interface JobBuilder {

  /**
   * Marks this job as signal-waiting. The job is created in {@link JobStatus#WAITING} status and
   * will not execute until a signal is delivered via {@link
   * JobSchedulerService#deliverSignal(java.util.UUID, java.io.Serializable) deliverSignal(jobId,
   * payload)} or {@link JobSchedulerService#deliverSignal(String, java.io.Serializable)
   * deliverSignal(signalKey, payload)}. Structured approval/rejection decisions may also be
   * delivered with {@link JobSchedulerService#deliverSignal(java.util.UUID, SignalDecision)
   * deliverSignal(jobId, decision)}.
   *
   * <p>If the signal is not delivered within {@code timeout}, timeout handling happens
   * asynchronously: the stored job is retried or transitions to FAILED with a {@link
   * SignalTimeoutException}. This method throws only for invalid signal-wait configuration.
   *
   * @param signalKey the named signal this job waits for; used for broadcast delivery
   * @param timeout maximum wait duration before the job fails; must be positive
   * @throws IllegalArgumentException if {@code signalKey} is blank or {@code timeout} is null or
   *     non-positive
   */
  JobBuilder awaitSignal(String signalKey, Duration timeout);

  /** Returns the signal key set via {@link #awaitSignal}, or null if not configured. */
  String awaitSignalKey();

  /**
   * Returns the signal timeout duration set via {@link #awaitSignal}, or null if not configured.
   */
  Duration awaitSignalTimeout();

  /** Adds a workflow branch with a human-readable description for monitoring. */
  @Incubating
  JobBuilder branch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Marks this job for immediate execution notification, bypassing the adaptive polling delay.
   *
   * <p>Jobs with CRITICAL priority or zero delay are treated as immediate automatically.
   */
  JobBuilder immediate();

  /**
   * Registers a callback invoked after this job fails.
   *
   * @param handler failure callback; receives the job context and failure
   * @return this builder
   */
  JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> handler);

  /**
   * Registers a callback invoked after this job succeeds.
   *
   * @param handler success callback; receives the job context
   * @return this builder
   */
  JobBuilder onSuccess(SerializableConsumer<JobContext> handler);

  /**
   * Persists the job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   */
  JobHandle submit();

  /** Appends a task to the execution chain. */
  JobBuilder then(SerializableCheckedRunnable next);

  /** Schedules a separate job to run if this job fails. */
  @Incubating
  JobBuilder thenOnFailure(SerializableCheckedRunnable next);

  /** Schedules a separate job to run if this job succeeds. */
  @Incubating
  JobBuilder thenOnSuccess(SerializableCheckedRunnable next);

  /** Schedules a job when a predicate on {@link JobResult} is true. */
  @Incubating
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a job when a predicate on {@link JobResult} is true.
   *
   * @param priority evaluation order when multiple conditions overlap (lower = first)
   */
  @Incubating
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition,
      SerializableCheckedRunnable next,
      int priority);

  /** Schedules a job based on the job's return value. */
  @Incubating
  <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a job based on the job's return value.
   *
   * @param priority evaluation order when multiple conditions overlap (lower = first)
   */
  @Incubating
  <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next, int priority);

  /**
   * Overrides the auto-generated idempotency key with a custom key.
   *
   * <p>A UUID is auto-generated at builder creation time. Use this when an external ID (e.g. a
   * webhook delivery ID or payment request ID) should map to exactly one job permanently. Unlike
   * {@link #withBusinessKey(String)}, once consumed this key is never reusable.
   *
   * @param key if null or blank, the auto-generated UUID is kept
   */
  JobBuilder withIdempotencyKey(String key);

  /**
   * Prevents concurrent execution against the same entity.
   *
   * <p>Unlike {@link #withIdempotencyKey(String)}, multiple completed jobs may share the same key;
   * only active (PENDING/RUNNING) jobs are blocked.
   *
   * @param key if null or blank, no concurrent execution blocking is performed
   */
  JobBuilder withBusinessKey(String key);

  /**
   * Acquires a permit from the named resource pool before execution; reschedules if at capacity.
   *
   * @param resourceName if null or blank, no resource limiting is applied
   */
  JobBuilder withResource(String resourceName);

  /** Sets the backoff policy and base delay for retries. */
  JobBuilder withBackoff(BackoffPolicy policy, Duration param);

  /** Sets the maximum number of retry attempts (must be &gt;= 0). */
  JobBuilder withMaxRetries(int retries);

  /** Adds a string parameter accessible via {@link JobContext#param}. */
  JobBuilder withParam(String key, String value);

  /** Sets the job execution priority. */
  JobBuilder withPriority(JobPriority priority);

  /** Adds tags to the job. Tags are trimmed and lowercased; null/blank values are ignored. */
  JobBuilder withTags(String... tags);

  /** Sets the maximum execution duration before the job is timed out and marked failed. */
  JobBuilder withTimeout(Duration timeout);

  /** Returns the chain tasks in addition order. The list is unmodifiable. */
  List<SerializableCheckedRunnable> chainTasks();

  /**
   * Returns the delay duration; never null (may be {@link Duration#ZERO}).
   *
   * <p>The delay is fixed when this builder is created by a {@link JobSubmitter} enqueue overload;
   * this fluent builder does not expose a delay mutator.
   */
  Duration delay();

  /**
   * Returns the idempotency key; never null (auto-generated UUID if not overridden via {@link
   * #withIdempotencyKey(String)}).
   */
  String idempotencyKey();

  /** Returns the business key, or null if not configured. */
  String businessKey();

  /** Returns the failure callback, or null if not configured. */
  SerializableBiConsumer<JobContext, Throwable> onFailure();

  /** Returns the success callback, or null if not configured. */
  SerializableConsumer<JobContext> onSuccess();

  /** Returns the immutable job options for this builder. */
  JobOptions opts();

  /** Returns the job parameters. The map is unmodifiable. */
  Map<String, String> params();

  /** Returns the normalized job tags. The list is unmodifiable. */
  List<String> tags();

  /** Returns the task payload configured for this builder. */
  SerializableCheckedRunnable task();

  /** Returns conditional workflow branches. The list is unmodifiable. */
  @Incubating
  List<WorkflowBranch> workflowBranches();

  /** Returns the resource name, or null if no resource permit is required. */
  String resourceName();

  /** Returns true when this job should wake the poller immediately after persistence. */
  boolean isImmediate();
}
