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

import java.time.Duration;
import java.util.Collection;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.PartitionedBatchBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowCondition;

/** Narrowed view of a partitioned batch. */
final class DefaultPartitionedBatchBuilder implements PartitionedBatchBuilder {
  private final DefaultBatchBuilder delegate;

  DefaultPartitionedBatchBuilder(DefaultBatchBuilder delegate) {
    this.delegate = delegate;
  }

  @Override
  public PartitionedBatchBuilder withIdempotencyKey(String key) {
    delegate.withIdempotencyKey(key);
    return this;
  }

  @Override
  public PartitionedBatchBuilder virtual() {
    delegate.virtual();
    return this;
  }

  @Override
  public PartitionedBatchBuilder platform() {
    delegate.platform();
    return this;
  }

  @Override
  public PartitionedBatchBuilder withBackoff(BackoffPolicy policy, Duration param) {
    delegate.withBackoff(policy, param);
    return this;
  }

  @Override
  public PartitionedBatchBuilder withMaxRetries(int retries) {
    delegate.withMaxRetries(retries);
    return this;
  }

  @Override
  public PartitionedBatchBuilder withTimeout(Duration timeout) {
    delegate.withTimeout(timeout);
    return this;
  }

  @Override
  public PartitionedBatchBuilder withCallbackOptions(JobOptions options) {
    delegate.withCallbackOptions(options);
    return this;
  }

  @Override
  public PartitionedBatchBuilder withResource(String resourceName) {
    delegate.withResource(resourceName);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next) {
    delegate.thenOnBatchFailure(next);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    delegate.thenWhenBatch(condition, next);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next) {
    delegate.thenWhenFailureCount(maxFailures, next);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next) {
    delegate.thenWhenSuccessRate(minRate, next);
    return this;
  }

  @Override
  public PartitionedBatchBuilder onProgress(SerializableConsumer<BatchContext> hook) {
    delegate.onProgress(hook);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    delegate.thenBranch(condition, next, description);
    return this;
  }

  @Override
  public PartitionedBatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next) {
    delegate.thenOnBatchSuccess(next);
    return this;
  }

  @Override
  public PartitionedBatchBuilder reopen(Collection<Integer> indexes) {
    delegate.reopen(indexes);
    return this;
  }

  @Override
  public JobHandle submit() {
    return delegate.submit();
  }
}
