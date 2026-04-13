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

/** Default {@link JobBuilder} implementation. */
public final class DefaultJobBuilder implements JobBuilder {

  private final JobSubmitter submitter;
  private final SerializableCheckedRunnable task;
  private final Duration delay;
  private final List<String> tags = new ArrayList<>();
  private final List<SerializableCheckedRunnable> chain = new ArrayList<>();
  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();
  private final Map<String, String> params = new HashMap<>();
  private JobOptions options = JobOptions.defaults();
  private SerializableConsumer<JobContext> onSuccess;
  private SerializableBiConsumer<JobContext, Throwable> onFailure;
  private boolean immediate = false;
  private String idempotencyKey;
  private String businessKey;
  private String resourceName;

  /** Factory entry point. A UUID idempotency key is auto-generated at creation time. */
  public static JobBuilder create(
      JobSubmitter submitter, SerializableCheckedRunnable task, Duration delay) {
    return new DefaultJobBuilder(submitter, task, delay);
  }

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
      String trimmed = key.trim();
      if (trimmed.length() > 36) {
        throw new IllegalArgumentException(
            "Idempotency key must be at most 36 characters, got " + trimmed.length());
      }
      this.idempotencyKey = trimmed;
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
