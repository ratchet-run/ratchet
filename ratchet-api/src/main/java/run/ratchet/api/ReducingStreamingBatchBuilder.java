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
package run.ratchet.api;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** A batch whose child results are folded into one value. */
@Incubating
public interface ReducingStreamingBatchBuilder<T extends Serializable, R> {
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withIdempotencyKey(String key);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> virtual();

  @Incubating
  ReducingStreamingBatchBuilder<T, R> platform();

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withBackoff(BackoffPolicy policy, Duration param);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withMaxRetries(int retries);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withTimeout(Duration timeout);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withCallbackOptions(JobOptions options);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withResource(String resourceName);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenOnBatchFailure(SerializableCheckedRunnable next);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> fromStream(Stream<T> stream);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> withChunkSize(int size);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> onProgress(Consumer<StreamingBatchContext> hook);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> onBatchProgress(SerializableConsumer<BatchContext> hook);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenOnBatchSuccess(SerializableConsumer<R> callback);

  @Incubating
  ReducingStreamingBatchBuilder<T, R> process(SerializableFunction<T, R> action);

  @Incubating
  JobHandle start();

  /** Checked-exception variant of process. */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> processChecked(SerializableCheckedFunction<T, R> action);
}
