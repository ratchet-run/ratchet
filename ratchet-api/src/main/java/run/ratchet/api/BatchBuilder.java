package run.ratchet.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Fluent builder for batch job execution with progress monitoring, conditional branching, and
 * failure handling.
 */
public interface BatchBuilder {

  /**
   * Applies a specified action to each item in the provided collection and returns the current
   * {@code BatchBuilder} instance for further workflow configuration.
   *
   * @param <T> the type of the elements in the collection; must implement {@link Serializable}
   * @param items the collection of items to process, where each item will be passed to the given
   *     action
   * @param action the operation to perform on each item in the collection, represented by a {@link
   *     SerializableConsumer} functional interface
   * @return this builder
   */
  <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action);

  /**
   * Registers a hook to monitor the progress of the batch operation.
   *
   * <p>The provided hook is a {@link SerializableConsumer} that accepts a {@link BatchContext}
   * object, which holds real-time information about the state of the batch execution. This method
   * allows users to implement custom logic to track progress, report metrics, or react to specific
   * states during the batch's lifecycle.
   *
   * @param hook a {@link SerializableConsumer} that processes the current {@link BatchContext},
   *     providing details such as total items, completed items, failed items, and percentage
   *     completed
   * @return this builder
   */
  BatchBuilder onProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Submits the configured batch job for execution and returns a handle to track it.
   *
   * <p>The returned {@link JobHandle} provides a unique identifier for the submitted job, enabling
   * job tracking, status querying, or correlation with job execution logs.
   *
   * @return a {@link JobHandle} representing the submitted job, containing its unique identifier
   */
  JobHandle submit();

  /**
   * Executes the specified action when the given workflow condition is met and returns the current
   * {@code BatchBuilder} instance for further configuration.
   *
   * <p>This method allows the user to branch the workflow execution based on a conditional check.
   * If the condition evaluates to true, the provided action is executed.
   *
   * @param condition the {@link WorkflowCondition} that determines whether the action should be
   *     executed
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute if the
   *     condition is met
   * @param description a textual description of the branch, providing context for debugging or
   *     logging purposes
   * @return this builder
   */
  BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /**
   * Specifies an action to be executed if the batch operation fails, and returns the current {@code
   * BatchBuilder} instance for further configuration.
   *
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute when the
   *     batch operation encounters a failure
   * @return this builder
   */
  BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Specifies an action to execute when the batch operation completes successfully. This method
   * allows chaining additional steps to be performed upon successful batch execution.
   *
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute when the
   *     batch operation succeeds
   * @return this builder
   */
  BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next);

  /**
   * Executes the specified action when the provided condition on the batch context is met and
   * returns the current {@code BatchBuilder} instance for further workflow configuration.
   *
   * <p>This method allows conditional execution of an action based on the state and progress of the
   * batch operation. The condition is evaluated against the {@code BatchContext}, which provides
   * key metrics such as completed items, failed items, and success rates. If the condition
   * evaluates to true, the specified action is executed.
   *
   * @param condition the {@link SerializablePredicate} that evaluates the {@link BatchContext} to
   *     determine whether the action should be performed
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute if the
   *     condition is met
   * @return this builder
   */
  BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Specifies an action to execute when the number of failures in the batch operation reaches the
   * given threshold, and returns the current {@code BatchBuilder} instance for further workflow
   * configuration.
   *
   * @param maxFailures the maximum number of failures that triggers the execution of the specified
   *     action
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute when the
   *     failure count reaches the specified threshold
   * @return this builder
   */
  BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Specifies an action to execute when the success rate of the batch operation meets or exceeds
   * the given threshold, and returns the current {@code BatchBuilder} instance for further workflow
   * configuration.
   *
   * @param minRate the minimum success rate (expressed as a decimal value between 0.0 and 1.0)
   *     required to trigger the execution of the specified action
   * @param next the {@link SerializableCheckedRunnable} representing the action to execute when the
   *     success rate condition is met
   * @return this builder
   */
  BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
