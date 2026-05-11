package run.ratchet.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Fluent builder for batch job execution with progress monitoring, conditional branching, and
 * failure handling.
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

  /** Registers a hook invoked after each child job completes with current batch progress. */
  BatchBuilder onProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Persists the batch job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   */
  JobHandle submit();

  /** Adds a conditional workflow branch with a description for debugging. */
  BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /** Schedules a task to run if the batch fails. */
  BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next);

  /** Schedules a task to run if the batch succeeds. */
  BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next);

  /** Schedules a task when a custom predicate on {@link BatchContext} is true. */
  BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /** Schedules a task when the failure count reaches {@code maxFailures}. */
  BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Schedules a task when the success rate meets or exceeds {@code minRate}.
   *
   * @param minRate 0.0 to 1.0
   */
  BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
