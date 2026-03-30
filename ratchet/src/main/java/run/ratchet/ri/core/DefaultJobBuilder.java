package run.ratchet.ri.core;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobResult;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link JobBuilder} for the ratchet reference implementation.
 *
 * <p>This class provides the concrete builder logic for creating and configuring individual jobs
 * with various execution options, error handling strategies, and workflow capabilities.
 *
 * <p>Note: DefaultJobBuilder instances are obtained through the {@link #create(JobSubmitter,
 * SerializableCheckedRunnable, Duration)} factory method and should not be instantiated directly.
 *
 * @see JobBuilder
 * @see JobSubmitter
 * @see JobOptions
 * @see JobHandle
 * @see WorkflowCondition
 */
public final class DefaultJobBuilder implements JobBuilder {

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
   * run.ratchet.api.JobType#CHAIN} category. This enables building multi-step workflows
   * where each step must complete successfully before the next begins.
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
   * <p>Auto-generated as a UUID when the DefaultJobBuilder is created. This ensures that if the
   * same DefaultJobBuilder instance is retried (e.g., due to transaction retry or network issues),
   * the UNIQUE constraint on idempotencyKey catches the duplicate and returns the existing job.
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
   * Creates a new DefaultJobBuilder with the specified submitter, task, and execution delay.
   *
   * <p>This factory method is the public entry point for creating DefaultJobBuilder instances from
   * outside the package (e.g., from the scheduler service). The submitter handles actual
   * persistence when {@link #submit()} is called.
   *
   * <p><b>Important:</b> A UUID is auto-generated for the idempotency key at creation time. This
   * ensures that if the same DefaultJobBuilder instance is retried (e.g., transaction retry), it
   * uses the same UUID = deduplicated. New DefaultJobBuilder = new UUID = new job.
   *
   * @param submitter the job submitter managing this job's lifecycle
   * @param task the primary task to be executed
   * @param delay the duration to wait before initial job execution
   * @return a new JobBuilder instance
   * @see JobSubmitter
   */
  public static JobBuilder create(
      JobSubmitter submitter, SerializableCheckedRunnable task, Duration delay) {
    return new DefaultJobBuilder(submitter, task, delay);
  }

  /**
   * Package-private constructor used internally.
   *
   * @param submitter the job submitter managing this job's lifecycle
   * @param task the primary task to be executed
   * @param delay the duration to wait before initial job execution
   */
  DefaultJobBuilder(JobSubmitter submitter, SerializableCheckedRunnable task, Duration delay) {
    this.submitter = submitter;
    this.task = task;
    this.delay = delay;
    // Auto-generate idempotency key at builder creation time (NOT at persist time!)
    // This ensures same builder retried = same UUID = deduplicated
    this.idempotencyKey = UUID.randomUUID().toString();
  }

  @Override
  public JobBuilder branch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    workflowBranches.add(WorkflowBranch.of(condition, next, description));
    return this;
  }

  @Override
  public JobBuilder immediate() {
    this.immediate = true;
    return this;
  }

  @Override
  public JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> f) {
    onFailure = f;
    return this;
  }

  @Override
  public JobBuilder onSuccess(SerializableConsumer<JobContext> s) {
    onSuccess = s;
    return this;
  }

  @Override
  public JobHandle submit() {
    return submitter.submit(this);
  }

  @Override
  public JobBuilder then(SerializableCheckedRunnable next) {
    chain.add(next);
    return this;
  }

  @Override
  public JobBuilder thenOnFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failure(), next));
    return this;
  }

  @Override
  public JobBuilder thenOnSuccess(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.success(), next));
    return this;
  }

  @Override
  public <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.custom(condition), next));
    return this;
  }

  @Override
  public <T> JobBuilder when(
      SerializablePredicate<JobResult<T>> condition,
      SerializableCheckedRunnable next,
      int priority) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.custom(condition, priority), next));
    return this;
  }

  @Override
  public <T> JobBuilder whenResult(
      SerializableFunction<T, Boolean> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.result(condition), next));
    return this;
  }

  @Override
  public JobBuilder withIdempotencyKey(String key) {
    if (key != null && !key.isBlank()) {
      this.idempotencyKey = key.trim();
    }
    // If null/blank, keep the auto-generated UUID
    return this;
  }

  @Override
  public JobBuilder withBusinessKey(String key) {
    this.businessKey = (key != null && !key.isBlank()) ? key.trim() : null;
    return this;
  }

  @Override
  public JobBuilder withResource(String resourceName) {
    this.resourceName =
        (resourceName != null && !resourceName.isBlank()) ? resourceName.trim() : null;
    return this;
  }

  @Override
  public JobBuilder withBackoff(BackoffPolicy policy, Duration param) {
    options = options.withBackoff(policy, param);
    return this;
  }

  @Override
  public JobBuilder withMaxRetries(int retries) {
    options = options.withMaxRetries(retries);
    return this;
  }

  @Override
  public JobBuilder withParam(String key, String value) {
    if (key != null && !key.isBlank() && value != null) {
      params.put(key.trim(), value);
    }
    return this;
  }

  @Override
  public JobBuilder withPriority(JobPriority priority) {
    options = options.withPriority(priority);
    return this;
  }

  @Override
  public JobBuilder withTags(String... tags) {
    for (String tag : tags) {
      if (tag != null && !tag.isBlank()) {
        this.tags.add(tag.trim().toLowerCase());
      }
    }
    return this;
  }

  @Override
  public JobBuilder withTimeout(Duration timeout) {
    options = options.withTimeout(timeout);
    return this;
  }

  @Override
  public List<SerializableCheckedRunnable> chainTasks() {
    return Collections.unmodifiableList(chain);
  }

  @Override
  public Duration delay() {
    return delay;
  }

  @Override
  public String idempotencyKey() {
    return idempotencyKey;
  }

  @Override
  public String businessKey() {
    return businessKey;
  }

  @Override
  public boolean isImmediate() {
    return immediate;
  }

  @Override
  public SerializableBiConsumer<JobContext, Throwable> onFailure() {
    return onFailure;
  }

  @Override
  public SerializableConsumer<JobContext> onSuccess() {
    return onSuccess;
  }

  @Override
  public JobOptions opts() {
    return options;
  }

  @Override
  public Map<String, String> params() {
    return Collections.unmodifiableMap(params);
  }

  @Override
  public List<String> tags() {
    return Collections.unmodifiableList(tags);
  }

  @Override
  public SerializableCheckedRunnable task() {
    return task;
  }

  @Override
  public List<WorkflowBranch> workflowBranches() {
    return Collections.unmodifiableList(workflowBranches);
  }

  @Override
  public String resourceName() {
    return resourceName;
  }
}
