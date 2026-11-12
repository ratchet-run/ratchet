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
package run.ratchet.ri.core;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchBuilder;
import run.ratchet.api.BatchContext;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.KeyRange;
import run.ratchet.api.PartitionedBatchBuilder;
import run.ratchet.api.ReducingBatchBuilder;
import run.ratchet.api.SerializableBinaryOperator;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.TypeRef;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.WorkflowCondition.ConditionType;
import run.ratchet.api.internal.IdempotencyKeyNormalizer;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobPayload;

/** {@inheritDoc} */
public class DefaultBatchBuilder implements BatchBuilder {

  /**
   * Synthetic picker recorded on the BATCH_PARENT hot row when the empty-batch shortcut
   * skip-executes the parent into terminal SUCCEEDED. No real worker ever owns this id.
   */
  public static final String BATCH_LIFECYCLE_NODE_ID = "ratchet:batch-lifecycle";

  private final String name;
  private final BatchSubmitter submitter;
  private final JobInvocationResolver jobInvocationResolver;

  private final List<ChildSpec> children = new ArrayList<>();
  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();
  private final BatchChildOptions childOptionState = new BatchChildOptions();
  private JobOptions callbackOptions = JobOptions.defaults();
  private SerializableConsumer<BatchContext> progressHook;
  private String executionTarget;
  private String idempotencyKey;
  private String childResourceName;
  private BatchReducer reducer;
  private boolean itemsConfigured;
  private String partitionKey;
  private List<KeyRange> partitions;
  private SerializableConsumer<KeyRange> partitionAction;
  private final Set<Integer> reopened = new HashSet<>();

  String partitionKey() {
    return partitionKey;
  }

  List<KeyRange> partitions() {
    return partitions;
  }

  Set<Integer> reopened() {
    return reopened;
  }

  JobPayload partitionPayload(KeyRange range) {
    return payload(partitionAction, List.of(range));
  }

  @Override
  public PartitionedBatchBuilder forEachPartition(
      String key, List<KeyRange> ranges, SerializableConsumer<KeyRange> action) {
    if (itemsConfigured || reducer != null || partitionKey != null)
      throw new IllegalStateException("Partition configuration must be exclusive");
    String normalized = IdempotencyKeyNormalizer.normalize(key);
    if (normalized == null) throw new IllegalArgumentException("Partition key must not be blank");
    if (ranges != null && ranges.size() > submitter.maxPartitions()) {
      throw new IllegalArgumentException("Too many partitions");
    }
    if (ranges == null || ranges.isEmpty() || ranges.stream().anyMatch(Objects::isNull))
      throw new IllegalArgumentException("Partitions must be nonempty and contain no nulls");
    List<KeyRange> sorted = new ArrayList<>(ranges);
    sorted.sort(Comparator.comparingLong(KeyRange::from));
    for (int i = 1; i < sorted.size(); i++)
      if (sorted.get(i - 1).to() > sorted.get(i).from())
        throw new IllegalArgumentException("Partition ranges overlap");
    partitionAction = Objects.requireNonNull(action, "action");
    partitions = List.copyOf(ranges);
    partitionKey = normalized;
    itemsConfigured = true;
    return new DefaultPartitionedBatchBuilder(this);
  }

  void reopen(Collection<Integer> indexes) {
    if (indexes == null) throw new IllegalArgumentException("indexes must not be null");
    for (Integer index : indexes)
      if (index == null || index < 0 || index >= partitions.size())
        throw new IllegalArgumentException("Invalid partition index");
    reopened.addAll(indexes);
  }

  private void requirePlainItems() {
    if (partitionKey != null) throw new IllegalStateException("Partitions are already configured");
  }

  private boolean successConfigured;

  BatchReducer reducer() {
    return reducer;
  }

  DefaultBatchBuilder(String name, BatchSubmitter submitter) {
    this(name, submitter, new DefaultJobInvocationResolver());
  }

  DefaultBatchBuilder(
      String name, BatchSubmitter submitter, JobInvocationResolver jobInvocationResolver) {
    this.name = name;
    this.submitter = submitter;
    this.jobInvocationResolver = jobInvocationResolver;
  }

  @Override
  public <R> ReducingBatchBuilder<R> reducing(R identity, SerializableBinaryOperator<R> combiner) {
    return reducingType(null, identity, combiner);
  }

  @Override
  public <R> ReducingBatchBuilder<R> reducing(
      TypeRef<R> type, R identity, SerializableBinaryOperator<R> combiner) {
    return reducingType(Objects.requireNonNull(type, "type").getType(), identity, combiner);
  }

  private <R> ReducingBatchBuilder<R> reducingType(
      Type type, R identity, SerializableBinaryOperator<R> combiner) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(combiner, "combiner");
    if (itemsConfigured || successConfigured || reducer != null) {
      throw new IllegalStateException(
          "reducing() must precede items, processing and success callbacks");
    }
    reducer = new BatchReducer(type, identity, runtimeResultPayload(combiner));
    return new DefaultReducingBatchBuilder<>(this);
  }

  void reducingInvocation(Object identity, JobInvocation combiner) {
    if (itemsConfigured || successConfigured || reducer != null)
      throw new IllegalStateException("reducing() must be first");
    reducer = new BatchReducer(null, identity, JobPayloadFactory.fromInvocation(combiner));
  }

  void resultCallback(Serializable callback) {
    JobPayload payload = runtimeResultPayload(Objects.requireNonNull(callback, "callback"));
    reducer.callback(payload);
    successConfigured = true;
    WorkflowBranch branch =
        new WorkflowBranch(
            WorkflowCondition.batchSuccess(),
            new InvocationAdapter(
                new JobInvocation(
                    payload.target(),
                    payload.method(),
                    payload.methodDescriptor(),
                    payload.isStatic(),
                    payload.args(),
                    payload.runtimeArgIndexes())));
    reducer.successBranch(branch);
    workflowBranches.add(branch);
  }

  private JobPayload runtimeResultPayload(Serializable callback) {
    if (callback instanceof InvocationAdapter adapter)
      return JobPayloadFactory.fromInvocation(adapter.invocation());
    return JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback));
  }

  <T extends Serializable, R> void resultItems(
      Collection<T> items, SerializableFunction<T, R> action) {
    requirePlainItems();
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(action, "action");
    reducer.childTarget(runtimeResultPayload(action));
    requirePlainItems();
    itemsConfigured = true;
    for (T item : items) children.add(new ChildSpec(payload(action, List.of(item))));
  }

  @Override
  public BatchBuilder withIdempotencyKey(String key) {
    this.idempotencyKey = IdempotencyKeyNormalizer.normalize(key);
    return this;
  }

  String idempotencyKey() {
    return idempotencyKey;
  }

  @Override
  public <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action) {
    requirePlainItems();
    itemsConfigured = true;
    for (T item : Objects.requireNonNull(items, "items")) {
      children.add(new ChildSpec(payload(action, List.of(item))));
    }
    return this;
  }

  @Override
  public BatchBuilder onProgress(SerializableConsumer<BatchContext> hook) {
    this.progressHook = hook;
    return this;
  }

  @Override
  public BatchBuilder virtual() {
    this.executionTarget = ExecutorTargets.VIRTUAL;
    return this;
  }

  @Override
  public BatchBuilder platform() {
    this.executionTarget = ExecutorTargets.PLATFORM;
    return this;
  }

  @Override
  public BatchBuilder withBackoff(BackoffPolicy policy, Duration param) {
    childOptionState.withBackoff(policy, param);
    return this;
  }

  @Override
  public BatchBuilder withMaxRetries(int retries) {
    childOptionState.withMaxRetries(retries);
    return this;
  }

  @Override
  public BatchBuilder withTimeout(Duration timeout) {
    childOptionState.withTimeout(timeout);
    return this;
  }

  @Override
  public BatchBuilder withCallbackOptions(JobOptions options) {
    callbackOptions = Objects.requireNonNull(options, "options");
    return this;
  }

  @Override
  public BatchBuilder withResource(String resourceName) {
    this.childResourceName = DefaultJobBuilder.normalizeResourceName(resourceName);
    return this;
  }

  String childResourceName() {
    return childResourceName;
  }

  @Override
  public JobHandle submit() {
    return submitter.submit(this);
  }

  @Override
  public BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    if (condition.type() == ConditionType.BATCH_SUCCESS) successConfigured = true;
    workflowBranches.add(new WorkflowBranch(condition, next, description));
    return this;
  }

  @Override
  public BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchFailure(), next));
    return this;
  }

  @Override
  public BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next) {
    if (reducer != null)
      throw new IllegalStateException("Use the reducing builder to register its result callback");
    successConfigured = true;
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchSuccess(), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchCustom(condition), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failureCount(maxFailures), next));
    return this;
  }

  @Override
  public BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.successRate(minRate), next));
    return this;
  }

  String name() {
    return name;
  }

  List<ChildSpec> children() {
    return children;
  }

  List<WorkflowBranch> workflowBranches() {
    return workflowBranches;
  }

  SerializableConsumer<BatchContext> progressHook() {
    return progressHook;
  }

  String executionTarget() {
    return executionTarget;
  }

  JobOptions callbackOptions() {
    return callbackOptions;
  }

  JobOptions childOptions() {
    return childOptionState.value();
  }

  /** Invocation-typed sibling of {@link #forEach}: each child persists the factory's invocation. */
  <T extends Serializable> void forEachInvocation(
      Collection<T> items, Function<T, JobInvocation> invocationFactory) {
    requirePlainItems();
    itemsConfigured = true;
    for (T item : Objects.requireNonNull(items, "items")) {
      children.add(
          new ChildSpec(
              JobPayloadFactory.requireTaskArguments(
                  JobPayloadFactory.fromInvocation(invocationFactory.apply(item)))));
    }
  }

  private JobPayload payload(Serializable callback) {
    return JobPayloadFactory.requireTaskArguments(
        JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback)));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return JobPayloadFactory.requireTaskArguments(
        JobPayloadFactory.fromInvocation(
            jobInvocationResolver.resolve(callback, runtimeArguments)));
  }

  record ChildSpec(JobPayload payload) {}
}
