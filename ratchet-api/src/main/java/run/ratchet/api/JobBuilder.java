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
 *     .tag("order-processing", "customer-123")
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

  // ========== Fluent Builder Methods ==========

  /**
   * Adds a workflow branch with a custom description for monitoring/debugging.
   *
   * @param condition the workflow condition
   * @param next the task to execute
   * @param description human-readable description of this branch
   * @return this builder
   */
  JobBuilder branch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Marks this job for immediate execution notification.
   *
   * <p>When a job is marked as immediate, the scheduler publishes a wakeup notification to all
   * cluster nodes, causing their pollers to immediately check for available work. This bypasses the
   * normal adaptive polling delay and is ideal for user-triggered actions where responsiveness is
   * important.
   *
   * <p>Note: Jobs with CRITICAL priority or zero delay are automatically treated as immediate. Use
   * this method explicitly when you need immediate behavior for other job configurations.
   *
   * @return this builder
   */
  JobBuilder immediate();

  /**
   * Sets a callback to be invoked if the job fails during execution.
   *
   * @param f the callback function to handle failure events. This function accepts a {@link
   *     JobContext} that represents the execution context of the failed job, and a {@link
   *     Throwable} that describes the error.
   * @return this builder
   */
  JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> f);

  /**
   * Sets a callback to be invoked upon successful completion of the job.
   *
   * @param s the callback function to handle successful completion events. This function accepts a
   *     {@link JobContext} that represents the execution context of the job.
   * @return this builder
   */
  JobBuilder onSuccess(SerializableConsumer<JobContext> s);

  /**
   * Submits the current job configuration, including the main task and any chained tasks, to the
   * job scheduler for persistence and execution.
   *
   * @return a {@link JobHandle} representing the submitted job, providing access to its unique
   *     identifier.
   */
  JobHandle submit();

  /**
   * Adds a new task to the chain of tasks to be executed as part of the job. This allows for
   * sequential execution of multiple tasks in the order they are added. Supports method chaining.
   *
   * @param next the task to be added to the chain. Must not be null.
   * @return this builder
   */
  JobBuilder then(SerializableCheckedRunnable next);

  /**
   * Schedules a separate job to execute if the current job fails. This creates a workflow branch
   * with a FAILURE condition.
   *
   * @param next the task to execute on failure as a separate job
   * @return this builder
   */
  JobBuilder thenOnFailure(SerializableCheckedRunnable next);

  /**
   * Schedules a separate job to execute if the current job succeeds. This creates a workflow branch
   * with a SUCCESS condition.
   *
   * @param next the task to execute on success as a separate job
   * @return this builder
   */
  JobBuilder thenOnSuccess(SerializableCheckedRunnable next);

  /**
   * Schedules a job to execute when a custom condition is met. The condition is evaluated based on
   * the JobResult of the current job.
   *
   * @param condition predicate that determines if the branch should execute
   * @param next the task to execute when condition is met
   * @param <T> the type of the job result
   * @return this builder
   */
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a job to execute when a custom condition with priority is met. Lower priority numbers
   * are evaluated first.
   *
   * @param condition predicate that determines if the branch should execute
   * @param next the task to execute when condition is met
   * @param priority evaluation priority (lower = higher priority)
   * @param <T> the type of the job result
   * @return this builder
   */
  <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition,
      SerializableCheckedRunnable next,
      int priority);

  /**
   * Schedules a job to execute based on the return value of the current job. The condition function
   * receives the job's return value and returns a boolean.
   *
   * @param condition function that evaluates the job's return value
   * @param next the task to execute when condition returns true
   * @param <T> the type of the job result
   * @return this builder
   */
  <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next);

  /**
   * Overrides the auto-generated idempotency key with a custom key.
   *
   * <p>By default, a UUID is auto-generated at JobBuilder creation time. Use this method when you
   * need custom idempotency semantics, such as:
   *
   * <ul>
   *   <li>Webhook delivery IDs (same delivery = same job forever)
   *   <li>Payment request IDs (same payment attempt = same job)
   *   <li>Any external ID that should map to exactly one job
   * </ul>
   *
   * <p><b>Difference from {@link #withBusinessKey(String)}:</b>
   *
   * <ul>
   *   <li>idempotencyKey is UNIQUE globally - once used, that key is consumed forever
   *   <li>businessKey only blocks active (PENDING/RUNNING) jobs - allows re-runs after completion
   * </ul>
   *
   * <h3>Example:</h3>
   *
   * <pre>{@code
   * // Webhook handler - same delivery ID = same job forever
   * scheduler.enqueue(() -> processWebhook(payload))
   *     .withIdempotencyKey(webhookDeliveryId)
   *     .submit();
   * }</pre>
   *
   * @param key the idempotency key. If null or blank, keeps the auto-generated UUID.
   * @return this builder
   */
  JobBuilder withIdempotencyKey(String key);

  /**
   * Sets a business key for preventing concurrent execution against the same entity.
   *
   * <p>Unlike {@link #withIdempotencyKey(String)} which is globally unique, businessKey allows
   * multiple completed jobs with the same key over time. It only blocks when an active
   * (PENDING/RUNNING) job exists with the same key.
   *
   * <p><b>Use cases:</b>
   *
   * <ul>
   *   <li>"Only one sync per user at a time, but re-runs allowed"
   *   <li>Prevent concurrent operations on the same business entity
   *   <li>Rate-limit job processing per entity
   * </ul>
   *
   * <h3>Example:</h3>
   *
   * <pre>{@code
   * // Only one sync per user - re-runs allowed after completion
   * scheduler.enqueue(() -> syncUser(userId))
   *     .withBusinessKey("sync-user-" + userId)
   *     .submit();
   * }</pre>
   *
   * @param key the business key. If null or blank, no concurrent execution blocking is performed.
   * @return this builder
   */
  JobBuilder withBusinessKey(String key);

  /**
   * Specifies a resource that this job requires for execution.
   *
   * <p>When a resource is specified, the job will attempt to acquire a permit from the resource
   * pool before execution begins. If no permits are available (resource at capacity), the job will
   * be rescheduled with a configurable delay.
   *
   * <p>This enables limiting concurrent access to shared resources regardless of job type. For
   * example, limiting concurrent calls to a payment API to 5 total, even if those calls come from
   * different job types.
   *
   * <h3>Example:</h3>
   *
   * <pre>{@code
   * // Limit concurrent payment API calls across all job types
   * scheduler.enqueue(() -> processPayment(paymentId))
   *     .withResource("payment-api")
   *     .submit();
   * }</pre>
   *
   * @param resourceName the name of the resource to acquire. If null or blank, no resource
   *     limiting.
   * @return this builder
   */
  JobBuilder withResource(String resourceName);

  /**
   * Configures the job to use the specified backoff policy and parameter. The backoff policy
   * determines the delay strategy applied between retries, while the parameter specifies the value
   * required by the selected strategy.
   *
   * @param policy the backoff policy to apply. Must not be null. Supported policies include {@code
   *     NONE}, {@code FIXED}, and {@code EXPONENTIAL}.
   * @param param the parameter for the backoff strategy. For example, in the {@code FIXED}
   *     strategy, this defines the fixed delay between retries. Must not be null.
   * @return this builder
   */
  JobBuilder withBackoff(BackoffPolicy policy, Duration param);

  /**
   * Configures the maximum number of retry attempts for the job in case of failure. This method
   * allows chaining for further job configuration.
   *
   * @param retries the maximum number of retries to attempt. Must be a non-negative integer.
   * @return this builder
   */
  JobBuilder withMaxRetries(int retries);

  /**
   * Adds a parameter to the job that can be accessed during execution. Parameters are simple
   * key-value pairs that provide configuration data to the job without the overhead of serializing
   * complex objects.
   *
   * @param key the parameter key. Must not be null or blank.
   * @param value the parameter value. Must not be null.
   * @return this builder
   */
  JobBuilder withParam(String key, String value);

  /**
   * Sets the priority of the job. Priority determines the execution order of jobs, with higher
   * priority jobs being executed before lower priority ones. This method allows method chaining for
   * further customization.
   *
   * @param priority the priority level for the job.
   * @return this builder
   */
  JobBuilder withPriority(JobPriority priority);

  /**
   * Adds one or more tags to the job configuration. Tags are trimmed, converted to lowercase, and
   * stored only if they are non-null and non-blank. This method supports chaining.
   *
   * @param tags the tags to add to the job. Each tag should be a non-null, non-blank string.
   * @return this builder
   */
  JobBuilder withTags(String... tags);

  /**
   * Sets a timeout for the job, specifying the maximum duration the job is allowed to run before
   * being considered as timed out. This method updates the internal job options with the specified
   * timeout value.
   *
   * @param timeout the timeout duration for the job. Must not be null.
   * @return this builder
   */
  JobBuilder withTimeout(Duration timeout);

  // ========== Accessor Methods ==========

  /** Returns the chain tasks in addition order. The list is unmodifiable. */
  List<SerializableCheckedRunnable> chainTasks();

  /**
   * Returns the configured delay before job execution.
   *
   * @return the delay duration, never null (may be {@link Duration#ZERO})
   * @see JobSubmitter
   */
  Duration delay();

  /**
   * Returns the idempotency key for duplicate job creation prevention.
   *
   * <p>This value is NEVER null - it's auto-generated at builder creation time.
   *
   * @return the idempotency key (auto-generated UUID or custom-provided)
   * @see JobSubmitter
   */
  String idempotencyKey();

  /**
   * Returns the business key for concurrent execution prevention, if set.
   *
   * @return the business key, or null if not configured
   * @see JobSubmitter
   */
  String businessKey();

  /**
   * Returns whether this job should trigger immediate wakeup notification.
   *
   * @return true if immediate wakeup is requested, false otherwise
   * @see JobSubmitter
   */
  boolean isImmediate();

  /**
   * Returns the configured failure callback.
   *
   * @return the failure callback, or null if not configured
   * @see JobSubmitter
   */
  SerializableBiConsumer<JobContext, Throwable> onFailure();

  /**
   * Returns the configured success callback.
   *
   * @return the success callback, or null if not configured
   * @see JobSubmitter
   */
  SerializableConsumer<JobContext> onSuccess();

  /**
   * Returns the configured job options.
   *
   * @return the job options, never null (defaults to {@link JobOptions#defaults()})
   * @see JobSubmitter
   */
  JobOptions opts();

  /**
   * Returns the configured job parameters.
   *
   * @return an unmodifiable view of the parameters map, never null
   * @see JobSubmitter
   */
  Map<String, String> params();

  /**
   * Returns the configured job tags.
   *
   * @return an unmodifiable view of the tags list, never null
   * @see JobSubmitter
   */
  List<String> tags();

  /**
   * Returns the primary task to be executed.
   *
   * @return the primary task, never null
   * @see JobSubmitter
   */
  SerializableCheckedRunnable task();

  /**
   * Retrieves the list of workflow branches for this job.
   *
   * @return an unmodifiable list of workflow branches
   */
  List<WorkflowBranch> workflowBranches();

  /**
   * Returns the resource name this job requires for execution, if any.
   *
   * @return the resource name, or null if no resource limiting is needed
   * @see JobSubmitter
   */
  String resourceName();
}
