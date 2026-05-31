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
import java.util.Collection;

/**
 * Fluent builder for batch job execution with progress monitoring, conditional branching, and
 * failure handling.
 *
 * @since 0.1
 */
@Incubating
public interface BatchBuilder {

  /**
   * Enqueues one child job per item in the collection, each processed by {@code action}.
   *
   * <p>The items collection must not be null. An empty collection creates an empty batch.
   *
   * @param <T> item payload type
   * @param items the items to process
   * @param action child job action invoked once per item
   * @return this builder
   * @throws NullPointerException if {@code items} is null
   */
  <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action);

  /**
   * Registers a hook invoked after each child job completes with current batch progress.
   *
   * @param hook progress callback; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code hook} is null
   */
  BatchBuilder onProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Routes this batch and its child jobs to the virtual executor pool ({@link
   * ExecutorTargets#VIRTUAL}).
   *
   * <p>Mutually exclusive with {@link #platform()}; last call wins. Calling neither leaves the
   * batch on the deployment's default threading mode.
   */
  @Incubating
  BatchBuilder virtual();

  /**
   * Routes this batch and its child jobs to the platform executor pool ({@link
   * ExecutorTargets#PLATFORM}).
   *
   * <p>Mutually exclusive with {@link #virtual()}; last call wins. Calling neither leaves the batch
   * on the deployment's default threading mode.
   */
  @Incubating
  BatchBuilder platform();

  /**
   * Persists the batch job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   */
  JobHandle submit();

  /**
   * Adds a conditional workflow branch with a description for debugging.
   *
   * @param condition branch predicate; must not be {@code null}
   * @param next task scheduled when the condition matches; must not be {@code null}
   * @param description optional label for monitoring and debugging
   * @return this builder
   * @throws NullPointerException if {@code condition} or {@code next} is null
   */
  BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Schedules a task to run if the batch fails.
   *
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code next} is null
   */
  BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Schedules a task to run if the batch succeeds.
   *
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code next} is null
   */
  BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next);

  /**
   * Schedules a task when a custom predicate on {@link BatchContext} is true.
   *
   * @param condition predicate evaluated against the batch context; must not be {@code null}
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code condition} or {@code next} is null
   */
  BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Schedules a task when the failure count reaches {@code maxFailures}.
   *
   * @param maxFailures failure threshold
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws NullPointerException if {@code next} is null
   */
  BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Schedules a task when the success rate meets or exceeds {@code minRate}.
   *
   * @param minRate success rate threshold in the range 0.0 to 1.0
   * @param next task to schedule; must not be {@code null}
   * @return this builder
   * @throws IllegalArgumentException if {@code minRate} is less than 0.0 or greater than 1.0
   * @throws NullPointerException if {@code next} is null
   */
  BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
