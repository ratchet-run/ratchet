package run.ratchet.api;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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

  /** Adds a workflow branch with a human-readable description for monitoring. */
  JobBuilder branch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Marks this job for immediate execution notification, bypassing the adaptive polling delay.
   *
   * <p>Jobs with CRITICAL priority or zero delay are treated as immediate automatically.
   */
  JobBuilder immediate();

  JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> f);

  JobBuilder onSuccess(SerializableConsumer<JobContext> s);

  /** Submits the job and returns a handle to it. */
  JobHandle submit();

  /** Appends a task to the execution chain. */
  JobBuilder then(SerializableCheckedRunnable next);

  /** Schedules a separate job to run if this job fails. */
  JobBuilder thenOnFailure(SerializableCheckedRunnable next);

  /** Schedules a separate job to run if this job succeeds. */
  JobBuilder thenOnSuccess(SerializableCheckedRunnable next);

  /** Schedules a job when a predicate on {@link JobResult} is true. */
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a job when a predicate on {@link JobResult} is true.
   *
   * @param priority evaluation order when multiple conditions overlap (lower = first)
   */
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition,
      SerializableCheckedRunnable next,
      int priority);

  /** Schedules a job based on the job's return value. */
  <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next);

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

  /** Returns the delay duration; never null (may be {@link Duration#ZERO}). */
  Duration delay();

  /**
   * Returns the idempotency key; never null (auto-generated UUID if not overridden via {@link
   * #withIdempotencyKey(String)}).
   */
  String idempotencyKey();

  /** Returns the business key, or null if not configured. */
  String businessKey();

  SerializableBiConsumer<JobContext, Throwable> onFailure();

  SerializableConsumer<JobContext> onSuccess();

  JobOptions opts();

  Map<String, String> params();

  List<String> tags();

  SerializableCheckedRunnable task();

  List<WorkflowBranch> workflowBranches();

  String resourceName();

  boolean isImmediate();
}
