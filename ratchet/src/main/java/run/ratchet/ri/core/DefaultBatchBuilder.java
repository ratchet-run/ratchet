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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchBuilder;
import run.ratchet.api.BatchContext;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
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
  private final BatchChildRetryOptions childRetryOptions = new BatchChildRetryOptions();
  private SerializableConsumer<BatchContext> progressHook;
  private String executionTarget;

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
  public <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action) {
    for (T item : items) {
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
    childRetryOptions.withBackoff(policy, param);
    return this;
  }

  @Override
  public BatchBuilder withMaxRetries(int retries) {
    childRetryOptions.withMaxRetries(retries);
    return this;
  }

  @Override
  public JobHandle submit() {
    return submitter.submit(this);
  }

  @Override
  public BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
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

  JobOptions childOptions() {
    return childRetryOptions.value();
  }

  /** Invocation-typed sibling of {@link #forEach}: each child persists the factory's invocation. */
  <T extends Serializable> void forEachInvocation(
      Collection<T> items, Function<T, JobInvocation> invocationFactory) {
    for (T item : items) {
      children.add(new ChildSpec(JobPayloadFactory.fromInvocation(invocationFactory.apply(item))));
    }
  }

  private JobPayload payload(Serializable callback) {
    return JobPayloadFactory.fromInvocation(jobInvocationResolver.resolve(callback));
  }

  private JobPayload payload(Serializable callback, List<Object> runtimeArguments) {
    return JobPayloadFactory.fromInvocation(
        jobInvocationResolver.resolve(callback, runtimeArguments));
  }

  record ChildSpec(JobPayload payload) {}
}
