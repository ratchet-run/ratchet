package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Serializable variant of {@link Predicate} for use in workflow conditions and filters.
 *
 * <p>This functional interface extends {@link Predicate} with {@link Serializable} capability,
 * enabling lambda expressions and method references that evaluate conditions to be persisted as
 * part of job workflows. It accepts one argument and returns a boolean result, making it ideal for
 * conditional branching, filtering, and decision points in job execution flows.
 *
 * <h2>Primary Use Cases:</h2>
 *
 * <ul>
 *   <li>Custom workflow conditions for job/batch results
 *   <li>Dynamic branching logic based on execution state
 *   <li>Filtering and validation in job chains
 *   <li>Conditional execution of workflow branches
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Custom job result conditions
 * scheduler.enqueue(() -> processData())
 *     .when(result -> result.isSuccess() && result.getExecutionTimeMs() < 5000,
 *           () -> log.info("Fast successful execution"))
 *     .when(result -> result.isFailure() && result.getError().contains("timeout"),
 *           () -> increaseTimeoutAndRetry())
 *     .submit();
 *
 * // Batch completion conditions
 * scheduler.enqueueBatch("Import Records")
 *     .forEach(records, record -> importRecord(record))
 *     .thenWhenBatch(context -> context.failedItems() == 0,
 *                    () -> markImportAsSuccessful())
 *     .thenWhenBatch(context -> context.failedItems() > context.totalItems() / 2,
 *                    () -> rollbackImport())
 *     .thenWhenBatch(context -> context.successRate() >= 0.95,
 *                    () -> acceptWithWarnings())
 *     .submit();
 *
 * // Complex conditions with method references
 * public class WorkflowManager {
 *     public JobHandle createConditionalWorkflow() {
 *         return scheduler.enqueue(this::analyzeSystem)
 *             .when(this::requiresImmediateAction, this::triggerAlert)
 *             .when(this::requiresScheduledMaintenance, this::scheduleMaintenance)
 *             .when(this::isOperatingNormally, this::logHealthCheck)
 *             .submit();
 *     }
 *
 *     private boolean requiresImmediateAction(JobResult<SystemAnalysis> result) {
 *         return result.isSuccess() &&
 *                result.getValue().getCriticalIssues() > 0;
 *     }
 *
 *     private boolean requiresScheduledMaintenance(JobResult<SystemAnalysis> result) {
 *         return result.isSuccess() &&
 *                result.getValue().getMaintenanceScore() > 0.7;
 *     }
 *
 *     private boolean isOperatingNormally(JobResult<SystemAnalysis> result) {
 *         return result.isSuccess() &&
 *                result.getValue().getHealthScore() > 0.9;
 *     }
 * }
 *
 * // Combining multiple conditions
 * SerializablePredicate<BatchContext> criticalBatchCondition =
 *     context -> context.failedItems() > 10 ||
 *                context.successRate() < 0.5 ||
 *                !context.isComplete();
 *
 * scheduler.enqueueBatch("Critical Process")
 *     .forEach(items, item -> processItem(item))
 *     .thenWhenBatch(criticalBatchCondition, () -> escalateToOps())
 *     .submit();
 * }</pre>
 *
 * <h2>Best Practices:</h2>
 *
 * <ul>
 *   <li>Keep predicate logic simple and readable
 *   <li>Avoid side effects in predicate evaluations
 *   <li>Handle null inputs defensively
 *   <li>Use descriptive method names when using method references
 *   <li>Combine simple predicates for complex conditions
 * </ul>
 *
 * @param <T> the type of the input to the predicate
 * @see JobBuilder#when(SerializablePredicate, SerializableCheckedRunnable)
 * @see BatchBuilder#thenWhenBatch(SerializablePredicate, SerializableCheckedRunnable)
 * @see WorkflowCondition#custom(SerializablePredicate)
 * @see WorkflowCondition#batchCustom(SerializablePredicate)
 */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  /**
   * Evaluates this predicate on the given argument.
   *
   * <p>This method is inherited from {@link Predicate} and enhanced with serialization support.
   * Implementations should evaluate the input and return a boolean result without side effects for
   * predictable behavior in workflow conditions.
   *
   * <p>When used in job scheduling contexts, both the lambda implementation and any captured
   * variables must be serializable to allow persistence in job queues.
   *
   * @param t the input argument
   * @return {@code true} if the input argument matches the predicate, otherwise {@code false}
   */
  @Override
  boolean test(T t);
}
