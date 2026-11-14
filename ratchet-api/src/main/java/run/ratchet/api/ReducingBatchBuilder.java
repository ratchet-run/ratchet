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
import run.ratchet.api.exception.PayloadTooLargeException;

/** A batch whose child results are folded into one value. */
@Incubating
public interface ReducingBatchBuilder<R> {
  /**
   * Sets the idempotency key of the batch parent to deduplicate batch submissions.
   *
   * <p>The key is trimmed and must contain at most 36 printable ASCII characters ({@code U+0020}
   * through {@code U+007E}). Null or blank keeps the generated key. Children and branch jobs keep
   * generated keys. The key namespace is shared with single jobs.
   *
   * <p>A later submission with the same key returns the handle of the job that holds the key, which
   * is a single job if one used it first, and writes no parent, children, progress hook or
   * branches.
   *
   * <p>If two submissions race on the same key, the loser's {@link #submit()} may throw. A retry in
   * a new transaction returns the original handle, even after job history is purged. Task execution
   * remains at least once.
   *
   * @param key if null or blank, the auto-generated UUID is kept
   * @return this builder
   * @throws IllegalArgumentException if the trimmed key is longer than 36 characters or contains a
   *     character outside printable ASCII
   */
  @Incubating
  ReducingBatchBuilder<R> withIdempotencyKey(String key);

  /**
   * Routes this batch and its child jobs to the virtual executor pool ({@link
   * ExecutorTargets#VIRTUAL}).
   *
   * <p>Mutually exclusive with {@link #platform()}; last call wins. Calling neither leaves the
   * batch on the deployment's default threading mode.
   */
  @Incubating
  ReducingBatchBuilder<R> virtual();

  /**
   * Routes this batch and its child jobs to the platform executor pool ({@link
   * ExecutorTargets#PLATFORM}).
   *
   * <p>Mutually exclusive with {@link #virtual()}; last call wins. Calling neither leaves the batch
   * on the deployment's default threading mode.
   */
  @Incubating
  ReducingBatchBuilder<R> platform();

  /**
   * Sets the retry backoff policy and base delay for every child job in this batch.
   *
   * <p>The setting applies to the whole builder, including children already added through {@link
   * #forEach}. It does not apply to the no-op batch parent or workflow branches.
   *
   * @param policy the backoff strategy applied between child attempts
   * @param param policy-specific base delay
   * @return this builder
   * @throws NullPointerException if {@code policy} or {@code param} is null
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  @Incubating
  ReducingBatchBuilder<R> withBackoff(BackoffPolicy policy, Duration param);

  /**
   * Sets the maximum number of retry attempts for every child job in this batch.
   *
   * <p>The default is {@code 0} for compatibility. The setting applies to the whole builder,
   * including children already added through {@link #forEach}. It does not apply to the no-op batch
   * parent or workflow branches.
   *
   * @param retries maximum child retry attempts; {@code 0} disables retries
   * @return this builder
   * @throws IllegalArgumentException if {@code retries} is negative
   * @throws UnsupportedOperationException if the builder does not support child retry settings
   */
  @Incubating
  ReducingBatchBuilder<R> withMaxRetries(int retries);

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
  ReducingBatchBuilder<R> withTimeout(Duration timeout);

  /**
   * Sets the priority, retries, backoff and timeout of jobs created through thenOnBatchSuccess,
   * thenOnBatchFailure, thenWhenSuccessRate, thenWhenFailureCount, thenWhenBatch and thenBranch.
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
  ReducingBatchBuilder<R> withCallbackOptions(JobOptions options);

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
   * jobs. {@code submit()} throws {@link UnsupportedOperationException} if a resource is set and
   * the store does not advertise the {@code ResourcePermitStore} capability.
   *
   * @param resourceName if null or blank, no resource limiting is applied (the name is trimmed
   *     otherwise)
   * @return this builder
   */
  @Incubating
  ReducingBatchBuilder<R> withResource(String resourceName);

  /**
   * Schedules a task to run if the batch fails.
   *
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code next} is null
   */
  @Incubating
  ReducingBatchBuilder<R> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Schedules a task when a custom predicate on {@link BatchContext} is true.
   *
   * @param condition predicate evaluated against the batch context; must not be {@code null}
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code condition} or {@code next} is null
   */
  @Incubating
  ReducingBatchBuilder<R> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a task when the final failure count does not exceed {@code maxFailures}.
   *
   * @param maxFailures maximum allowed failures, inclusive
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws IllegalArgumentException if maxFailures is negative
   * @throws NullPointerException if {@code next} is null
   */
  @Incubating
  ReducingBatchBuilder<R> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Schedules a task when the success rate meets or exceeds {@code minRate}.
   *
   * @param minRate success rate threshold in the range 0.0 to 1.0
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws IllegalArgumentException if {@code minRate} is less than 0.0 or greater than 1.0
   * @throws NullPointerException if {@code next} is null
   */
  @Incubating
  ReducingBatchBuilder<R> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);

  /**
   * Registers a hook invoked after each child job completes with current batch progress.
   *
   * @param hook progress callback; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code hook} is null
   */
  @Incubating
  ReducingBatchBuilder<R> onProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Adds a conditional workflow branch with a description for debugging.
   *
   * <p>A BATCH_SUCCESS branch does not receive the combined result. Use thenOnBatchSuccess to
   * receive it.
   *
   * @param condition branch predicate; must not be {@code null}
   * @param next task scheduled when the condition matches; must not be {@code null}
   * @param description optional label for monitoring and debugging
   * @return this builder
   * @throws NullPointerException if {@code condition} or {@code next} is null
   */
  @Incubating
  ReducingBatchBuilder<R> thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Registers the one success callback that receives the combined result.
   *
   * <p>The callback parameter type must equal R exactly, including generic arguments. Unlike
   * BatchBuilder's success task, this callback receives the result.
   *
   * @param callback callback receiving the combined result; must not be null
   * @return this builder
   * @throws NullPointerException if callback is null
   * @throws IllegalStateException if a result callback was already registered
   * @throws IllegalArgumentException if the callback does not declare exactly one result slot
   */
  @Incubating
  ReducingBatchBuilder<R> thenOnBatchSuccess(SerializableConsumer<R> callback);

  /**
   * Adds one child per item, each returning a partial result of type R.
   *
   * <p>A child with nothing to report must return the identity. A null, missing or truncated
   * partial from a successful child fails the batch. Failed children contribute nothing. An empty
   * collection produces the identity. Unlike BatchBuilder, the action returns a result.
   *
   * @param <T> item payload type
   * @param items items to process
   * @param action action returning each child's partial result
   * @return this builder
   * @throws NullPointerException if items or action is null
   */
  @Incubating
  <T extends Serializable> ReducingBatchBuilder<R> forEach(
      Collection<T> items, SerializableFunction<T, R> action);

  /**
   * Persists the batch job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   *
   * @throws PayloadTooLargeException if a child, progress hook, or workflow payload exceeds the
   *     configured serialized-payload limit
   * @throws UnsupportedOperationException if a resource is set and the store does not advertise the
   *     {@code ResourcePermitStore} capability
   */
  @Incubating
  JobHandle submit();
}
