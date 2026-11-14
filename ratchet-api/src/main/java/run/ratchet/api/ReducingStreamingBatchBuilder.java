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
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.api.exception.RatchetTransientStoreException;

/** A batch whose child results are folded into one value. */
@Incubating
public interface ReducingStreamingBatchBuilder<T extends Serializable, R> {
  /**
   * Sets the idempotency key of the batch parent to deduplicate batch submissions.
   *
   * <p>The key is trimmed and must contain at most 36 printable ASCII characters ({@code U+0020}
   * through {@code U+007E}). Null or blank keeps the generated key. Children and branch jobs keep
   * generated keys. The key namespace is shared with single jobs.
   *
   * <p>A later submission with the same key returns the handle of the job that holds the key, which
   * is a single job if one used it first, and writes no parent, children, progress hook or
   * branches. The stream is closed without being read.
   *
   * <p>If two submissions race on the same key, the loser's {@link #start()} may throw. A retry in
   * a new transaction returns the original handle, even after job history is purged. Task execution
   * remains at least once.
   *
   * @param key if null or blank, the auto-generated UUID is kept
   * @return this builder
   * @throws IllegalArgumentException if the trimmed key is longer than 36 characters or contains a
   *     character outside printable ASCII
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withIdempotencyKey(String key);

  /**
   * Routes this streaming batch and its child jobs to the virtual executor pool ({@link
   * ExecutorTargets#VIRTUAL}).
   *
   * <p>Mutually exclusive with {@link #platform()}; last call wins. Calling neither leaves the
   * batch on the deployment's default threading mode.
   *
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> virtual();

  /**
   * Routes this streaming batch and its child jobs to the platform executor pool ({@link
   * ExecutorTargets#PLATFORM}).
   *
   * <p>Mutually exclusive with {@link #virtual()}; last call wins. Calling neither leaves the batch
   * on the deployment's default threading mode.
   *
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> platform();

  /**
   * Sets the retry backoff policy and base delay for every child job in this batch.
   *
   * <p>The setting applies to the whole builder regardless of call order. It does not apply to the
   * no-op batch parent or workflow branches.
   *
   * @param policy the backoff strategy applied between child attempts
   * @param param policy-specific base delay
   * @return this builder
   * @throws NullPointerException if {@code policy} or {@code param} is null
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withBackoff(BackoffPolicy policy, Duration param);

  /**
   * Sets the maximum number of retry attempts for every child job in this batch.
   *
   * <p>The default is {@code 0} for compatibility. The setting applies to the whole builder
   * regardless of call order. It does not apply to the no-op batch parent or workflow branches.
   *
   * @param retries maximum child retry attempts; {@code 0} disables retries
   * @return this builder
   * @throws IllegalArgumentException if {@code retries} is negative
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withMaxRetries(int retries);

  /**
   * Sets the execution timeout for every child job in this batch.
   *
   * <p>Null (the default) leaves children on the global default SLA. The setting applies to the
   * whole builder, including children already added. It does not apply to the batch parent or to
   * workflow branch and callback jobs.
   *
   * @param timeout child execution timeout; null clears the per-child timeout
   * @return this builder
   * @throws IllegalArgumentException if {@code timeout} is negative, is positive but shorter than
   *     one second, or exceeds {@link Integer#MAX_VALUE} whole seconds
   * @throws UnsupportedOperationException if the builder does not support child timeout settings
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withTimeout(Duration timeout);

  /**
   * Sets the priority, retries, backoff and timeout of jobs created through thenOnBatchSuccess,
   * thenOnBatchFailure, thenWhenSuccessRate, thenWhenFailureCount and thenWhenBatch.
   *
   * <p>The default is {@link JobOptions#defaults()}. Child settings from {@code withMaxRetries},
   * {@code withBackoff} and {@code withTimeout} do not apply to these jobs. This setting applies to
   * the whole builder regardless of call order. A callback that inherits retries also uses this
   * backoff.
   *
   * @param options callback job options; never null
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withCallbackOptions(JobOptions options);

  /**
   * Makes every child job acquire a permit from the named resource pool before it runs.
   *
   * <p>When the pool is at capacity, the child is rescheduled without counting as a retry. The
   * configured permit count caps how many children of this batch run at once and is shared with any
   * other job, inside or outside this batch, using the same resource name. Permit counts are
   * configured per resource name in the store's resource-limit table {@code
   * scheduler_resource_limit} with columns {@code max_concurrent} and {@code retry_delay_ms}; see
   * the performance-tuning deployment guide.
   *
   * <p>This setting applies to the whole builder regardless of call order, including children
   * already added. It does not apply to the no-op batch parent, progress hooks, or workflow branch
   * jobs. {@code start()} throws {@link UnsupportedOperationException} if a resource is set and the
   * store does not advertise the {@code ResourcePermitStore} capability.
   *
   * @param resourceName if null or blank, no resource limiting is applied (the name is trimmed
   *     otherwise)
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withResource(String resourceName);

  /**
   * Executes the given task when a batch fails.
   *
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if next is null
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a custom batch condition is met.
   *
   * @param condition predicate evaluated against the {@link BatchContext}
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if condition or next is null
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the final failure count is less than or equal to {@code
   * maxFailures}.
   *
   * @param maxFailures maximum allowed failures, must be non-negative
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws IllegalArgumentException if {@code maxFailures} is negative
   * @throws NullPointerException if next is null
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the success rate meets or exceeds the threshold.
   *
   * @param minRate minimum success rate (0.0 to 1.0)
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if next is null
   * @throws IllegalArgumentException if minRate is outside 0.0 to 1.0
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next);

  /**
   * Sets the input stream of items to process. The stream item type must match this builder.
   *
   * <p>A stream with no items creates an empty batch that completes as succeeded when submitted.
   * Its workflow branches are routed as for a batch with a 100% success rate and zero failures.
   * Routing is exclusive: the first matching branch runs, and every other branch is canceled.
   *
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> fromStream(Stream<T> stream);

  /**
   * Sets the number of items per chunk.
   *
   * @param size positive chunk size
   * @return this builder
   * @throws IllegalArgumentException if {@code size} is less than 1
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> withChunkSize(int size);

  /**
   * Attaches a local progress hook invoked during stream consumption with current streaming
   * metrics.
   *
   * <p>This hook is not serialized with the batch and does not run after handoff to another JVM.
   * Use {@link #onBatchProgress(SerializableConsumer)} for persisted batch execution progress.
   *
   * @param hook receives a {@link StreamingBatchContext} with streaming progress
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> onProgress(Consumer<StreamingBatchContext> hook);

  /**
   * Attaches a batch-level progress hook invoked during job execution with batch metrics.
   *
   * @param hook receives a {@link BatchContext} with batch execution progress
   * @return this builder
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> onBatchProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Registers the one success callback that receives the combined result.
   *
   * <p>The callback parameter type must equal R exactly, including generic arguments. Unlike
   * StreamingBatchBuilder's success task, this callback receives the result.
   *
   * @param callback callback receiving the combined result; must not be null
   * @return this builder
   * @throws NullPointerException if callback is null
   * @throws IllegalStateException if a result callback was already registered
   * @throws IllegalArgumentException if the callback does not declare exactly one result slot
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> thenOnBatchSuccess(SerializableConsumer<R> callback);

  /**
   * Sets the action returning each child's partial result of type R.
   *
   * <p>This replaces any previous process or processChecked action; the last call wins. A child
   * with nothing to report must return the identity. A null, missing or truncated partial from a
   * successful child fails the batch. Failed children contribute nothing. Unlike
   * StreamingBatchBuilder, the action returns a result.
   *
   * @param action action returning each child's partial result; must not be null
   * @return this builder
   * @throws NullPointerException if action is null
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> process(SerializableFunction<T, R> action);

  /**
   * Submits the configured streaming batch for execution.
   *
   * <p>The stream is closed before this method returns or throws.
   *
   * <p>The stream is closed on every path, including validation, duplicate submissions and
   * persistence failures. The invocation form of a reducing stream loads all items into memory and
   * validates their invocations before writing any row.
   *
   * @return a {@link JobHandle} for the submitted batch job; never {@code null}
   * @throws RatchetTransientStoreException if the backing store is temporarily unavailable while
   *     the batch is submitted
   * @throws PayloadTooLargeException if a child, progress hook, or workflow payload exceeds the
   *     configured serialized-payload limit
   * @throws IllegalStateException if no input stream or processing action has been configured
   * @throws UnsupportedOperationException if a resource is set and the store does not advertise the
   *     {@code ResourcePermitStore} capability
   */
  @Incubating
  JobHandle start();

  /**
   * Sets a partial-result action that may throw checked exceptions.
   *
   * <p>This replaces any previous process or processChecked action; the last call wins. The result
   * contract is the same as {@link #process(SerializableFunction)}.
   *
   * @param action action returning each child's partial result; must not be null
   * @return this builder
   * @throws NullPointerException if action is null
   */
  @Incubating
  ReducingStreamingBatchBuilder<T, R> processChecked(SerializableCheckedFunction<T, R> action);
}
