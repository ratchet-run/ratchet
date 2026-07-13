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

  /** Sets the input stream of items to process. The stream item type must match this builder. */
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
   * Submits the configured streaming batch for execution.
   *
   * @return a {@link JobHandle} for the submitted batch job; never {@code null}
   * @throws RatchetTransientStoreException if the backing store is temporarily unavailable while
   *     the batch is submitted
   * @throws PayloadTooLargeException if a child, progress hook, or workflow payload exceeds the
   *     configured serialized-payload limit
   * @throws IllegalStateException if no input stream or processing action has been configured
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
