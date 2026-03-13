package run.ratchet.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Fluent API builder for creating and configuring individual jobs.
 *
 * <p>JobBuilder provides a comprehensive and flexible way to create jobs with various execution
 * options, error handling strategies, and workflow capabilities. It supports job chaining,
 * conditional branching, retry policies, and lifecycle callbacks.
 *
 * <h2>Key Features:</h2>
 *
 * <ul>
 *   <li>Fluent API for intuitive job configuration
 *   <li>Job chaining for sequential task execution
 *   <li>Configurable retry policies with backoff strategies
 *   <li>Success and failure callbacks
 *   <li>Job tagging for categorization and filtering
 *   <li>Priority-based execution scheduling
 *   <li>Timeout configuration for long-running tasks
 *   <li>Workflow branching for conditional execution paths
 * </ul>
 *
 * <h2>Basic Usage Example:</h2>
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
 * <h2>Job Chaining Example:</h2>
 *
 * <pre>{@code
 * schedulerService.enqueue(() -> validateData())
 *     .then(() -> processData())
 *     .then(() -> generateReport())
 *     .then(() -> sendNotification())
 *     .submit();
 * }</pre>
 *
 * <h2>Workflow Branching Example:</h2>
 *
 * <pre>{@code
 * schedulerService.enqueue(() -> analyzeData())
 *     .thenOnSuccess(() -> archiveResults())
 *     .thenOnFailure(() -> notifyAdmins())
 *     .whenResult(result -> result.getScore() > 0.8,
 *                 () -> triggerHighScoreWorkflow())
 *     .submit();
 * }</pre>
 *
 * <p>Note: JobBuilder instances are obtained through {@link JobSubmitter} methods and should not be
 * instantiated directly.
 *
 * @see JobSubmitter
 * @see JobOptions
 * @see JobHandle
 * @see WorkflowCondition
 */
public final class JobBuilder {

  /**
   * Reference to the job submitter that will persist and execute the job.
   *
   * <p>This submitter handles the actual persistence of the job configuration to the database and
   * manages the job lifecycle.
   */
  private final JobSubmitter submitter;

  /**
   * The primary task to be executed when the job runs.
   *
   * <p>This is the main work unit of the job. It must be serializable to allow persistence and
   * execution on any cluster node.
   */
  private final SerializableCheckedRunnable task;

  /**
   * The duration to wait before the job becomes eligible for execution.
   *
   * <p>A delay of {@link Duration#ZERO} means the job is immediately eligible. Non-zero delays are
   * used for scheduled/deferred job execution.
   */
  private final Duration delay;

  /**
   * Tags for categorizing and filtering jobs.
   *
   * <p>Tags are normalized to lowercase during addition. They enable querying jobs by category
   * (e.g., "order-processing", "email-notifications") for monitoring and management purposes.
   */
  private final List<String> tags = new ArrayList<>();

  /**
   * Sequential chain of tasks to execute after the primary task completes.
   *
   * <p>Tasks in the chain execute in order as linked internal jobs within the public {@link
   * JobType#CHAIN} category. This enables building multi-step workflows where each step must
   * complete successfully before the next begins.
   */
  private final List<SerializableCheckedRunnable> chain = new ArrayList<>();

  /**
   * Conditional workflow branches that may execute based on job results.
   *
   * <p>Unlike the sequential chain, workflow branches are evaluated against conditions (success,
   * failure, custom predicates) and may or may not execute depending on the job's outcome. Multiple
   * branches can execute if their conditions are met.
   */
  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();

  /**
   * Key-value parameters that can be accessed during job execution.
   *
   * <p>Parameters provide a lightweight way to pass configuration data to jobs without the overhead
   * of serializing complex objects. Accessed via {@link JobContext#param(String)} and {@link
   * JobContext#param(String, String)}.
   */
  private final Map<String, String> params = new HashMap<>();

  /**
   * Configuration options for job execution behavior.
   *
   * <p>Includes priority, retry settings, backoff policy, and timeout configuration. Defaults to
   * {@link JobOptions#defaults()} and can be customized through the various {@code with*} methods.
   */
  private JobOptions options = JobOptions.defaults();

  /**
   * Callback invoked when the job completes successfully.
   *
   * <p>Receives the {@link JobContext} allowing access to job metadata and parameters. The callback
   * is serialized with the job payload and executed on the same thread immediately after successful
   * task completion.
   */
  private SerializableConsumer<JobContext> onSuccess;

  /**
   * Callback invoked when the job fails after exhausting all retries.
   *
   * <p>Receives both the {@link JobContext} and the exception that caused the failure. The callback
   * is serialized with the job payload and executed on the same thread after final failure
   * determination.
   */
  private SerializableBiConsumer<JobContext, Throwable> onFailure;

  /**
   * Flag indicating whether this job should trigger immediate wakeup notification.
   *
   * <p>When true, the scheduler publishes a wakeup notification to all cluster nodes upon job
   * submission, causing pollers to immediately check for work. This reduces latency for
   * user-triggered actions where responsiveness is important.
   */
  private boolean immediate = false;

  /**
   * Idempotency key for preventing duplicate job creation from retries.
   *
   * <p>Auto-generated as a UUID when the JobBuilder is created. This ensures that if the same
   * JobBuilder instance is retried (e.g., due to transaction retry or network issues), the UNIQUE
   * constraint on idempotencyKey catches the duplicate and returns the existing job.
   *
   * <p>Callers can override with a custom key via {@link #withIdempotencyKey(String)} for external
   * deduplication (e.g., webhook delivery IDs, payment request IDs).
   */
  private String idempotencyKey;

  /**
   * Business key for preventing concurrent execution against the same entity.
   *
   * <p>Unlike {@link #idempotencyKey} which is globally unique, businessKey allows multiple
   * completed jobs with the same key over time. It only blocks creating a new job when an active
   * (PENDING/RUNNING) job with the same key already exists.
   *
   * <p>Use cases:
   *
   * <ul>
   *   <li>Prevent double-processing from UI double-clicks
   *   <li>"Only one sync per user at a time, but re-runs allowed after completion"
   *   <li>Prevent concurrent operations on the same business entity
   * </ul>
   */
  private String businessKey;

  /**
   * Name of the resource this job requires for execution.
   *
   * <p>When set, the job must acquire a permit from the resource pool before execution. If no
   * permits are available (resource at capacity), the job is rescheduled with a delay.
   *
   * <p>Use cases:
   *
   * <ul>
   *   <li>Limit concurrent calls to an external API (e.g., max 5 payment calls)
   *   <li>Prevent overload of rate-limited services
   *   <li>Share capacity across different job types accessing the same resource
   * </ul>
   */
  private String resourceName;

  /**
   * Creates a new JobBuilder with the specified submitter, task, and execution delay.
   *
   * <p>This factory method is the public entry point for creating JobBuilder instances from outside
   * the API package (e.g., from the reference implementation module). The submitter handles actual
   * persistence when {@link #submit()} is called.
   *
   * <p><b>Important:</b> A UUID is auto-generated for the idempotency key at creation time. This
   * ensures that if the same JobBuilder instance is retried (e.g., transaction retry), it uses the
   * same UUID = deduplicated. New JobBuilder = new UUID = new job.
   *
   * @param submitter the job submitter managing this job's lifecycle
   * @param task the primary task to be executed
   * @param delay the duration to wait before initial job execution
   * @return a new JobBuilder instance
   * @see JobSubmitter
   */
  public static JobBuilder create(
      JobSubmitter submitter, SerializableCheckedRunnable task, Duration delay) {
    return new JobBuilder(submitter, task, delay);
  }

  /**
   * Package-private constructor used internally.
   *
   * @param submitter the job submitter managing this job's lifecycle
   * @param task the primary task to be executed
   * @param delay the duration to wait before initial job execution
   */
  JobBuilder(JobSubmitter submitter, SerializableCheckedRunnable task, Duration delay) {
    this.submitter = submitter;
    this.task = task;
    this.delay = delay;
    // Auto-generate idempotency key at builder creation time (NOT at persist time!)
    // This ensures same builder retried = same UUID = deduplicated
    this.idempotencyKey = UUID.randomUUID().toString();
  }

  /**
   * Adds a workflow branch with a custom description for monitoring/debugging.
   *
   * @param condition the workflow condition
   * @param next the task to execute
   * @param description human-readable description of this branch
   * @return the current JobBuilder instance for method chaining
   */
  public JobBuilder branch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    workflowBranches.add(WorkflowBranch.of(condition, next, description));
    return this;
  }

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
   * @return the current {@code JobBuilder} instance, allowing method chaining
   */
  public JobBuilder immediate() {
    this.immediate = true;
    return this;
  }

  /**
   * Sets a callback to be invoked if the job fails during execution.
   *
   * @param f the callback function to handle failure events. This function accepts a {@link
   *     JobContext} that represents the execution context of the failed job, and a {@link
   *     Throwable} that describes the error.
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> f) {
    onFailure = f;
    return this;
  }

  /**
   * Sets a callback to be invoked upon successful completion of the job.
   *
   * @param s the callback function to handle successful completion events. This function accepts a
   *     {@link JobContext} that represents the execution context of the job.
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder onSuccess(SerializableConsumer<JobContext> s) {
    onSuccess = s;
    return this;
  }

  /**
   * Submits the current job configuration, including the main task and any chained tasks, to the
   * job scheduler for persistence and execution.
   *
   * @return a {@link JobHandle} representing the submitted job, providing access to its unique
   *     identifier.
   */
  public JobHandle submit() {
    return submitter.submit(this);
  }

  /**
   * Adds a new task to the chain of tasks to be executed as part of the job. This allows for
   * sequential execution of multiple tasks in the order they are added. Supports method chaining.
   *
   * @param next the task to be added to the chain. Must not be null.
   * @return the current {@code JobBuilder} instance, allowing further configuration.
   */
  public JobBuilder then(SerializableCheckedRunnable next) {
    chain.add(next);
    return this;
  }

  /**
   * Schedules a separate job to execute if the current job fails. This creates a workflow branch
   * with a FAILURE condition.
   *
   * @param next the task to execute on failure as a separate job
   * @return the current JobBuilder instance for method chaining
   */
  public JobBuilder thenOnFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failure(), next));
    return this;
  }

  /**
   * Schedules a separate job to execute if the current job succeeds. This creates a workflow branch
   * with a SUCCESS condition.
   *
   * @param next the task to execute on success as a separate job
   * @return the current JobBuilder instance for method chaining
   */
  public JobBuilder thenOnSuccess(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.success(), next));
    return this;
  }

  /**
   * Schedules a job to execute when a custom condition is met. The condition is evaluated based on
   * the JobResult of the current job.
   *
   * @param condition predicate that determines if the branch should execute
   * @param next the task to execute when condition is met
   * @return the current JobBuilder instance for method chaining
   */
  public <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.custom(condition), next));
    return this;
  }

  /**
   * Schedules a job to execute when a custom condition with priority is met. Lower priority numbers
   * are evaluated first.
   *
   * @param condition predicate that determines if the branch should execute
   * @param next the task to execute when condition is met
   * @param priority evaluation priority (lower = higher priority)
   * @return the current JobBuilder instance for method chaining
   */
  public <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition,
      SerializableCheckedRunnable next,
      int priority) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.custom(condition, priority), next));
    return this;
  }

  // ========== Workflow Branching Methods ==========

  /**
   * Schedules a job to execute based on the return value of the current job. The condition function
   * receives the job's return value and returns a boolean.
   *
   * @param condition function that evaluates the job's return value
   * @param next the task to execute when condition returns true
   * @return the current JobBuilder instance for method chaining
   */
  public <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.result(condition), next));
    return this;
  }

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
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withIdempotencyKey(String key) {
    if (key != null && !key.isBlank()) {
      this.idempotencyKey = key.trim();
    }
    // If null/blank, keep the auto-generated UUID
    return this;
  }

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
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withBusinessKey(String key) {
    this.businessKey = (key != null && !key.isBlank()) ? key.trim() : null;
    return this;
  }

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
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withResource(String resourceName) {
    this.resourceName =
        (resourceName != null && !resourceName.isBlank()) ? resourceName.trim() : null;
    return this;
  }

  /**
   * Configures the job to use the specified backoff policy and parameter. The backoff policy
   * determines the delay strategy applied between retries, while the parameter specifies the value
   * required by the selected strategy.
   *
   * @param policy the backoff policy to apply. Must not be null. Supported policies include {@code
   *     NONE}, {@code FIXED}, and {@code EXPONENTIAL}.
   * @param param the parameter for the backoff strategy. For example, in the {@code FIXED}
   *     strategy, this defines the fixed delay between retries. Must not be null.
   * @return the current {@code JobBuilder} instance, allowing further configuration.
   */
  public JobBuilder withBackoff(BackoffPolicy policy, Duration param) {
    options = options.withBackoff(policy, param);
    return this;
  }

  /**
   * Configures the maximum number of retry attempts for the job in case of failure. This method
   * allows chaining for further job configuration.
   *
   * @param retries the maximum number of retries to attempt. Must be a non-negative integer.
   * @return the current {@code JobBuilder} instance, allowing further configuration.
   */
  public JobBuilder withMaxRetries(int retries) {
    options = options.withMaxRetries(retries);
    return this;
  }

  /**
   * Adds a parameter to the job that can be accessed during execution. Parameters are simple
   * key-value pairs that provide configuration data to the job without the overhead of serializing
   * complex objects.
   *
   * @param key the parameter key. Must not be null or blank.
   * @param value the parameter value. Must not be null.
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withParam(String key, String value) {
    if (key != null && !key.isBlank() && value != null) {
      params.put(key.trim(), value);
    }
    return this;
  }

  /**
   * Sets the priority of the job. Priority determines the execution order of jobs, with higher
   * priority jobs being executed before lower priority ones. This method allows method chaining for
   * further customization.
   *
   * @param priority the priority level for the job.
   * @return the current {@code JobBuilder} instance, allowing further configuration.
   */
  public JobBuilder withPriority(JobPriority priority) {
    options = options.withPriority(priority);
    return this;
  }

  /**
   * Adds one or more tags to the job configuration. Tags are trimmed, converted to lowercase, and
   * stored only if they are non-null and non-blank. This method supports chaining.
   *
   * @param tags the tags to add to the job. Each tag should be a non-null, non-blank string.
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withTags(String... tags) {
    for (String tag : tags) {
      if (tag != null && !tag.isBlank()) {
        this.tags.add(tag.trim().toLowerCase());
      }
    }
    return this;
  }

  /**
   * Sets a timeout for the job, specifying the maximum duration the job is allowed to run before
   * being considered as timed out. This method updates the internal job options with the specified
   * timeout value.
   *
   * @param timeout the timeout duration for the job. Must not be null.
   * @return the current {@code JobBuilder} instance, allowing method chaining.
   */
  public JobBuilder withTimeout(Duration timeout) {
    options = options.withTimeout(timeout);
    return this;
  }

  /**
   * Retrieves the list of tasks that have been added to the job's execution chain.
   *
   * <p>The returned list is immutable and provides the tasks in the order they were added via
   * {@link #then(SerializableCheckedRunnable)}.
   *
   * @return an unmodifiable list of {@code SerializableCheckedRunnable} objects representing the
   *     task chain
   */
  public List<SerializableCheckedRunnable> chainTasks() {
    return Collections.unmodifiableList(chain);
  }

  /**
   * Returns the configured delay before job execution.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to calculate
   * the scheduled execution time.
   *
   * @return the delay duration, never null (may be {@link Duration#ZERO})
   */
  public Duration delay() {
    return delay;
  }

  /**
   * Returns the idempotency key for duplicate job creation prevention.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence. This value is
   * NEVER null - it's auto-generated at builder creation time.
   *
   * @return the idempotency key (auto-generated UUID or custom-provided)
   */
  public String idempotencyKey() {
    return idempotencyKey;
  }

  /**
   * Returns the business key for concurrent execution prevention, if set.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} to check for active jobs with the same
   * business key before creating a new job.
   *
   * @return the business key, or null if not configured
   */
  public String businessKey() {
    return businessKey;
  }

  /**
   * Returns whether this job should trigger immediate wakeup notification.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} to determine whether to publish a
   * wakeup notification to cluster nodes after job submission.
   *
   * @return true if immediate wakeup is requested, false otherwise
   */
  public boolean isImmediate() {
    return immediate;
  }

  /**
   * Returns the configured failure callback.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to serialize
   * the callback as part of the job payload.
   *
   * @return the failure callback, or null if not configured
   */
  public SerializableBiConsumer<JobContext, Throwable> onFailure() {
    return onFailure;
  }

  /**
   * Returns the configured success callback.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to serialize
   * the callback as part of the job payload.
   *
   * @return the success callback, or null if not configured
   */
  public SerializableConsumer<JobContext> onSuccess() {
    return onSuccess;
  }

  /**
   * Returns the configured job options.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to apply
   * priority, retry, timeout, and backoff settings to the job entity.
   *
   * @return the job options, never null (defaults to {@link JobOptions#defaults()})
   */
  public JobOptions opts() {
    return options;
  }

  /**
   * Returns the configured job parameters.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to store
   * parameters with the job entity.
   *
   * @return the mutable parameters map, never null
   */
  public Map<String, String> params() {
    return params;
  }

  /**
   * Returns the configured job tags.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to associate
   * tags with the job entity.
   *
   * @return the mutable list of tags, never null
   */
  public List<String> tags() {
    return tags;
  }

  /**
   * Returns the primary task to be executed.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to serialize
   * the task as the job payload.
   *
   * @return the primary task, never null
   */
  public SerializableCheckedRunnable task() {
    return task;
  }

  /**
   * Retrieves the list of workflow branches for this job.
   *
   * @return an unmodifiable list of workflow branches
   */
  public List<WorkflowBranch> workflowBranches() {
    return Collections.unmodifiableList(workflowBranches);
  }

  /**
   * Returns the resource name this job requires for execution, if any.
   *
   * <p>Package-private accessor used by {@link JobSubmitter} during job persistence to store the
   * resource requirement with the job entity.
   *
   * @return the resource name, or null if no resource limiting is needed
   */
  public String resourceName() {
    return resourceName;
  }
}
