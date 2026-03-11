package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable variant of {@link Function} for use in workflow conditions and transformations.
 *
 * <p>This functional interface extends {@link Function} with {@link Serializable} capability,
 * enabling lambda expressions and method references that transform values to be persisted as part
 * of job workflows. It accepts one argument and produces a result, making it ideal for conditional
 * logic, data transformations, and result-based branching.
 *
 * <h2>Primary Use Cases:</h2>
 *
 * <ul>
 *   <li>Workflow conditions based on job return values
 *   <li>Result transformations and mappings
 *   <li>Dynamic decision making in job chains
 *   <li>Value extraction for conditional branching
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Workflow branching based on return value
 * scheduler.enqueue(() -> analyzeData())
 *     .whenResult(score -> score > 0.8,
 *                () -> triggerHighPriorityWorkflow())
 *     .whenResult(score -> score > 0.5 && score <= 0.8,
 *                () -> triggerMediumPriorityWorkflow())
 *     .whenResult(score -> score <= 0.5,
 *                () -> triggerLowPriorityWorkflow())
 *     .submit();
 *
 * // Complex condition with data extraction
 * scheduler.enqueue(() -> fetchUserData())
 *     .whenResult(userData -> userData.getSubscriptionLevel() == Premium.GOLD,
 *                () -> sendPremiumFeatures())
 *     .whenResult(userData -> userData.getDaysUntilExpiry() < 7,
 *                () -> sendRenewalReminder())
 *     .submit();
 *
 * // Method reference for cleaner code
 * public class DataProcessor {
 *     public JobHandle processWithConditionalFlow() {
 *         return scheduler.enqueue(this::analyzeDataQuality)
 *             .whenResult(this::isHighQuality, this::performDetailedAnalysis)
 *             .whenResult(this::needsCleaning, this::cleanAndReprocess)
 *             .submit();
 *     }
 *
 *     private Boolean isHighQuality(DataQualityReport report) {
 *         return report.getErrorRate() < 0.01 &&
 *                report.getCompleteness() > 0.95;
 *     }
 *
 *     private Boolean needsCleaning(DataQualityReport report) {
 *         return report.getErrorRate() > 0.1 ||
 *                report.hasMissingRequiredFields();
 *     }
 * }
 *
 * // Chaining with transformations
 * scheduler.enqueue(() -> generateReport())
 *     .whenResult(report -> report.getTotalRevenue(),
 *                revenue -> revenue > 1000000,
 *                () -> notifyExecutives())
 *     .submit();
 * }</pre>
 *
 * <h2>Best Practices:</h2>
 *
 * <ul>
 *   <li>Keep transformation logic pure and side-effect free
 *   <li>Return meaningful boolean values for conditions
 *   <li>Handle null inputs gracefully
 *   <li>Use method references for complex logic
 * </ul>
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 * @see JobBuilder#whenResult(SerializableFunction, SerializableCheckedRunnable)
 * @see WorkflowCondition#result(SerializableFunction)
 * @see SerializablePredicate
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

  /**
   * Applies this function to the given argument.
   *
   * <p>This method is inherited from {@link Function} and enhanced with serialization support.
   * Implementations should transform the input value and return a result without side effects for
   * predictable behavior in workflow conditions.
   *
   * <p>When used in job scheduling contexts, both the lambda implementation and any captured
   * variables must be serializable to allow persistence in job queues.
   *
   * @param t the function argument
   * @return the function result
   */
  @Override
  R apply(T t);
}
