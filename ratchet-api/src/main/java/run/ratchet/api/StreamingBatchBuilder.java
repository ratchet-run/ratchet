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
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.api.exception.RatchetTransientStoreException;

/**
 * Fluent builder for streaming batch jobs that process large datasets in chunks.
 *
 * <p>Each item type must be {@link Serializable} because streaming batches persist chunk boundaries
 * and may replay individual items after failures. The stream itself is consumed by the builder and
 * is not serialized, but every emitted item and persisted batch callback must remain serializable
 * across scheduler restarts.
 *
 * @param <T> the item type, must be {@link Serializable}
 * @since 0.1
 */
@Incubating
public interface StreamingBatchBuilder<T extends Serializable> {
  /**
   * Folds partial child results, starting with a non-null identity. The combiner must be
   * deterministic and free of side effects: it may run more than once. Children with nothing to
   * report must return the identity; null results fail the batch. R is the combiner's declared
   * return type and must contain no type variables or wildcards.
   *
   * @throws IllegalStateException if items, processing or a success callback were already set
   */
  @Incubating
  default <R> ReducingStreamingBatchBuilder<T, R> reducing(
      R identity, SerializableBinaryOperator<R> combiner) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(combiner, "combiner");
    throw new UnsupportedOperationException("Batch reduction is not supported");
  }

  /** Explicit result-type form of reducing, supporting generic combiner methods. */
  @Incubating
  default <R> ReducingStreamingBatchBuilder<T, R> reducing(
      TypeRef<R> type, R identity, SerializableBinaryOperator<R> combiner) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(combiner, "combiner");
    throw new UnsupportedOperationException("Batch reduction is not supported");
  }

  /**
   * Sets the idempotency key of the batch parent to deduplicate batch submissions.
   *
   * <p>The key is trimmed and must be at most 36 characters. Null or blank keeps the generated key.
   * Children and branch jobs keep generated keys. The key namespace is shared with single jobs.
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
   * @throws IllegalArgumentException if the trimmed key is longer than 36 characters
   */
  StreamingBatchBuilder<T> withIdempotencyKey(String key);

  /**
   * Sets the input stream of items to process. The stream item type must match this builder.
   *
   * <p>A stream with no items creates an empty batch that completes as succeeded when submitted.
   * Its workflow branches are routed as for a batch with a 100% success rate and zero failures.
   * Routing is exclusive: the first matching branch runs, and every other branch is canceled.
   */
  StreamingBatchBuilder<T> fromStream(Stream<T> stream);

  /** Sets the processing action applied to each item; may throw checked exceptions. */
  StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action);

  /**
   * Sets the number of items per chunk.
   *
   * @param size positive chunk size
   * @throws IllegalArgumentException if {@code size} is less than 1
   */
  StreamingBatchBuilder<T> withChunkSize(int size);

  /**
   * Attaches a local progress hook invoked during stream consumption with current streaming
   * metrics.
   *
   * <p>This hook is not serialized with the batch and does not run after handoff to another JVM.
   * Use {@link #onBatchProgress(SerializableConsumer)} for persisted batch execution progress.
   *
   * @param hook receives a {@link StreamingBatchContext} with streaming progress
   */
  StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook);

  /**
   * Attaches a batch-level progress hook invoked during job execution with batch metrics.
   *
   * @param hook receives a {@link BatchContext} with batch execution progress
   */
  StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Routes this streaming batch and its child jobs to the virtual executor pool ({@link
   * ExecutorTargets#VIRTUAL}).
   *
   * <p>Mutually exclusive with {@link #platform()}; last call wins. Calling neither leaves the
   * batch on the deployment's default threading mode.
   */
  @Incubating
  StreamingBatchBuilder<T> virtual();

  /**
   * Routes this streaming batch and its child jobs to the platform executor pool ({@link
   * ExecutorTargets#PLATFORM}).
   *
   * <p>Mutually exclusive with {@link #virtual()}; last call wins. Calling neither leaves the batch
   * on the deployment's default threading mode.
   */
  @Incubating
  StreamingBatchBuilder<T> platform();

  /**
   * Sets the retry backoff policy and base delay for every child job in this batch.
   *
   * <p>The setting applies to the whole builder regardless of call order. It does not apply to the
   * no-op batch parent or workflow branches.
   *
   * @param policy the backoff strategy applied between child attempts
   * @param param policy-specific base delay
   * @throws NullPointerException if {@code policy} or {@code param} is null
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  default StreamingBatchBuilder<T> withBackoff(BackoffPolicy policy, Duration param) {
    Objects.requireNonNull(policy, "policy");
    Objects.requireNonNull(param, "param");
    throw new UnsupportedOperationException("Streaming batch child retry backoff is not supported");
  }

  /**
   * Sets the maximum number of retry attempts for every child job in this batch.
   *
   * <p>The default is {@code 0} for compatibility. The setting applies to the whole builder
   * regardless of call order. It does not apply to the no-op batch parent or workflow branches.
   *
   * @param retries maximum child retry attempts; {@code 0} disables retries
   * @throws IllegalArgumentException if {@code retries} is negative
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  default StreamingBatchBuilder<T> withMaxRetries(int retries) {
    if (retries < 0) {
      throw new IllegalArgumentException("retries must be at least 0");
    }
    throw new UnsupportedOperationException("Streaming batch child retries are not supported");
  }

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
  default StreamingBatchBuilder<T> withTimeout(Duration timeout) {
    throw new UnsupportedOperationException("Batch child timeouts are not supported");
  }

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
  default StreamingBatchBuilder<T> withCallbackOptions(JobOptions options) {
    Objects.requireNonNull(options, "options");
    throw new UnsupportedOperationException("Batch callback options are not supported");
  }

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
  StreamingBatchBuilder<T> withResource(String resourceName);

  /**
   * Submits the configured streaming batch for execution.
   *
   * <p>The stream is closed before this method returns or throws.
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
  JobHandle start();

  /**
   * Executes the given task when a batch completes successfully.
   *
   * @param next task to schedule; must not be {@code null}
   */
  StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a batch fails.
   *
   * @param next task to schedule; must not be {@code null}
   */
  StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a custom batch condition is met.
   *
   * @param condition predicate evaluated against the {@link BatchContext}
   * @param next task to schedule; must not be {@code null}
   */
  StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the final failure count is less than or equal to {@code
   * maxFailures}.
   *
   * @param maxFailures maximum allowed failures, must be non-negative
   * @param next task to schedule; must not be {@code null}
   * @throws IllegalArgumentException if {@code maxFailures} is negative
   */
  StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the success rate meets or exceeds the threshold.
   *
   * @param minRate minimum success rate (0.0 to 1.0)
   * @param next task to schedule; must not be {@code null}
   */
  StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
