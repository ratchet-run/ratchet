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
import java.util.Collection;

/** A batch whose child results are folded into one value. */
@Incubating
public interface ReducingBatchBuilder<R> {
  @Incubating
  ReducingBatchBuilder<R> withIdempotencyKey(String key);

  @Incubating
  ReducingBatchBuilder<R> virtual();

  @Incubating
  ReducingBatchBuilder<R> platform();

  @Incubating
  ReducingBatchBuilder<R> withBackoff(BackoffPolicy policy, Duration param);

  @Incubating
  ReducingBatchBuilder<R> withMaxRetries(int retries);

  @Incubating
  ReducingBatchBuilder<R> withTimeout(Duration timeout);

  @Incubating
  ReducingBatchBuilder<R> withCallbackOptions(JobOptions options);

  @Incubating
  ReducingBatchBuilder<R> withResource(String resourceName);

  @Incubating
  ReducingBatchBuilder<R> thenOnBatchFailure(SerializableCheckedRunnable next);

  @Incubating
  ReducingBatchBuilder<R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  @Incubating
  ReducingBatchBuilder<R> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  @Incubating
  ReducingBatchBuilder<R> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);

  @Incubating
  ReducingBatchBuilder<R> onProgress(SerializableConsumer<BatchContext> hook);

  @Incubating
  ReducingBatchBuilder<R> thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  @Incubating
  ReducingBatchBuilder<R> thenOnBatchSuccess(SerializableConsumer<R> callback);

  @Incubating
  <T extends Serializable> ReducingBatchBuilder<R> forEach(
      Collection<T> items, SerializableFunction<T, R> action);

  @Incubating
  JobHandle submit();
}
