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
import java.util.Collection;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.ReducingBatchBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowCondition;

/** Delegates reduction to the existing batch submission pipeline. */
final class DefaultReducingBatchBuilder<R> implements ReducingBatchBuilder<R> {
  private final DefaultBatchBuilder delegate;

  DefaultReducingBatchBuilder(DefaultBatchBuilder delegate) {
    this.delegate = delegate;
  }

  @Override
  public ReducingBatchBuilder<R> withIdempotencyKey(String key) {
    delegate.withIdempotencyKey(key);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> virtual() {
    delegate.virtual();
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> platform() {
    delegate.platform();
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> withBackoff(BackoffPolicy policy, Duration param) {
    delegate.withBackoff(policy, param);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> withMaxRetries(int retries) {
    delegate.withMaxRetries(retries);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> withTimeout(Duration timeout) {
    delegate.withTimeout(timeout);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> withCallbackOptions(JobOptions options) {
    delegate.withCallbackOptions(options);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> withResource(String resourceName) {
    delegate.withResource(resourceName);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenOnBatchFailure(SerializableCheckedRunnable next) {
    delegate.thenOnBatchFailure(next);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    delegate.thenWhenBatch(condition, next);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next) {
    delegate.thenWhenFailureCount(maxFailures, next);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next) {
    delegate.thenWhenSuccessRate(minRate, next);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> onProgress(SerializableConsumer<BatchContext> hook) {
    delegate.onProgress(hook);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description) {
    delegate.thenBranch(condition, next, description);
    return this;
  }

  @Override
  public ReducingBatchBuilder<R> thenOnBatchSuccess(SerializableConsumer<R> callback) {
    delegate.resultCallback(callback);
    return this;
  }

  @Override
  public <T extends Serializable> ReducingBatchBuilder<R> forEach(
      Collection<T> items, SerializableFunction<T, R> action) {
    delegate.resultItems(items, action);
    return this;
  }

  @Override
  public JobHandle submit() {
    return delegate.submit();
  }
}
