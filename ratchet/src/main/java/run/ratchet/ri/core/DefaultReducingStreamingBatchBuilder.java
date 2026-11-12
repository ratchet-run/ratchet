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
import java.util.function.Consumer;
import java.util.stream.Stream;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.ReducingStreamingBatchBuilder;
import run.ratchet.api.SerializableCheckedFunction;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.StreamingBatchContext;

/** Delegates reduction to the existing batch submission pipeline. */
final class DefaultReducingStreamingBatchBuilder<T extends Serializable, R>
    implements ReducingStreamingBatchBuilder<T, R> {
  private final DefaultStreamingBatchBuilder<T> delegate;

  DefaultReducingStreamingBatchBuilder(DefaultStreamingBatchBuilder<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withIdempotencyKey(String key) {
    delegate.withIdempotencyKey(key);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> virtual() {
    delegate.virtual();
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> platform() {
    delegate.platform();
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withBackoff(BackoffPolicy policy, Duration param) {
    delegate.withBackoff(policy, param);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withMaxRetries(int retries) {
    delegate.withMaxRetries(retries);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withTimeout(Duration timeout) {
    delegate.withTimeout(timeout);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withCallbackOptions(JobOptions options) {
    delegate.withCallbackOptions(options);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withResource(String resourceName) {
    delegate.withResource(resourceName);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> thenOnBatchFailure(SerializableCheckedRunnable next) {
    delegate.thenOnBatchFailure(next);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    delegate.thenWhenBatch(condition, next);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next) {
    delegate.thenWhenFailureCount(maxFailures, next);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next) {
    delegate.thenWhenSuccessRate(minRate, next);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> fromStream(Stream<T> stream) {
    delegate.fromStream(stream);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> withChunkSize(int size) {
    delegate.withChunkSize(size);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> onProgress(Consumer<StreamingBatchContext> hook) {
    delegate.onProgress(hook);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> onBatchProgress(
      SerializableConsumer<BatchContext> hook) {
    delegate.onBatchProgress(hook);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> thenOnBatchSuccess(SerializableConsumer<R> callback) {
    delegate.resultCallback(callback);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> process(SerializableFunction<T, R> action) {
    delegate.resultAction(action);
    return this;
  }

  @Override
  public ReducingStreamingBatchBuilder<T, R> processChecked(
      SerializableCheckedFunction<T, R> action) {
    delegate.resultAction(action);
    return this;
  }

  @Override
  public JobHandle start() {
    return delegate.start();
  }
}
