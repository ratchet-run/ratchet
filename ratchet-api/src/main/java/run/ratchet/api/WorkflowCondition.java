package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * Defines conditions for dynamic workflow branching in job execution.
 *
 * <p>WorkflowCondition represents the decision criteria that determine whether a workflow branch
 * should execute. It supports various condition types ranging from simple success/failure checks to
 * complex custom predicates based on job results or batch contexts. This enables sophisticated,
 * data-driven workflow orchestration.
 *
 * <h2>Condition Types:</h2>
 *
 * <dl>
 *   <dt><b>SUCCESS/FAILURE</b>
 *   <dd>Simple binary conditions based on job completion status
 *   <dt><b>CUSTOM</b>
 *   <dd>Complex conditions using predicates on JobResult objects
 *   <dt><b>RESULT_VALUE</b>
 *   <dd>Conditions based on the actual return value of a job
 *   <dt><b>BATCH_SUCCESS/BATCH_FAILURE</b>
 *   <dd>Batch-specific conditions for all-success or any-failure scenarios
 *   <dt><b>BATCH_SUCCESS_RATE</b>
 *   <dd>Threshold-based conditions on batch completion success percentage
 *   <dt><b>BATCH_FAILURE_COUNT</b>
 *   <dd>Conditions based on acceptable failure thresholds
 *   <dt><b>BATCH_CUSTOM</b>
 *   <dd>Complex batch conditions using predicates on BatchContext
 * </dl>
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Simple conditions
 * WorkflowCondition onSuccess = WorkflowCondition.success();
 * WorkflowCondition onFailure = WorkflowCondition.failure();
 *
 * // Value-based conditions
 * WorkflowCondition highScore = WorkflowCondition.result(
 *     score -> score > 90
 * );
 *
 * // Complex job result conditions
 * WorkflowCondition slowExecution = WorkflowCondition.custom(
 *     result -> result.isSuccess() &&
 *               result.getExecutionTimeMs() > 30000
 * );
 *
 * // Batch conditions
 * WorkflowCondition perfectBatch = WorkflowCondition.batchSuccess();
 * WorkflowCondition acceptableRate = WorkflowCondition.successRate(0.95);
 * WorkflowCondition lowFailures = WorkflowCondition.failureCount(5);
 *
 * // Custom batch condition with priority
 * WorkflowCondition criticalBatch = WorkflowCondition.batchCustom(
 *     context -> context.failedItems() > 10 &&
 *                context.percentDone() == 100,
 *     1  // Higher priority
 * );
 * }</pre>
 *
 * <h2>Priority System:</h2>
 *
 * <p>Conditions can have priorities (default 0) that determine evaluation order when multiple
 * conditions might match. Lower priority values are evaluated first. This allows for deterministic
 * workflow execution when conditions overlap.
 *
 * @param type The type of condition determining which evaluation strategy to use.
 *     <p>This field identifies how the condition should be evaluated (e.g., simple success/failure
 *     check, custom predicate evaluation, batch success rate threshold). The type determines which
 *     evaluator logic is applied during workflow branch processing.
 * @param expression The condition expression, which varies based on the condition type.
 *     <p>This field holds the type-specific expression data used during evaluation:
 *     <ul>
 *       <li>For SUCCESS/FAILURE/BATCH_SUCCESS/BATCH_FAILURE: null (no expression needed)
 *       <li>For CUSTOM: a {@link SerializablePredicate} on JobResult
 *       <li>For RESULT_VALUE: a {@link SerializableFunction} mapping result to Boolean
 *       <li>For BATCH_SUCCESS_RATE: a Double threshold (0.0 to 1.0)
 *       <li>For BATCH_FAILURE_COUNT: an Integer maximum failure count
 *       <li>For BATCH_CUSTOM: a {@link SerializablePredicate} on BatchContext
 *     </ul>
 *     <p>The expression must be Serializable because it is persisted along with the job payload and
 *     must survive serialization/deserialization cycles in the job queue.
 * @param priority The evaluation priority for ordering when multiple conditions might match.
 *     <p>When multiple workflow branches have conditions that evaluate to true, the priority
 *     determines the order in which they are processed. Lower priority values are evaluated and
 *     executed first (priority 0 executes before priority 1).
 *     <p>The default priority is 0. Use higher priority values (1, 2, etc.) to ensure certain
 *     branches are evaluated after others, or negative values (-1, -2) to ensure evaluation before
 *     default-priority branches.
 * @see JobBuilder#when(SerializablePredicate, SerializableCheckedRunnable)
 * @see BatchBuilder#thenWhenBatch(SerializablePredicate, SerializableCheckedRunnable)
 * @see WorkflowBranch
 */
public record WorkflowCondition(ConditionType type, Serializable expression, int priority)
    implements Serializable {

  /**
   * Serialization version identifier for ensuring compatibility during deserialization.
   *
   * <p>This field is required because WorkflowCondition instances are persisted as part of workflow
   * branches in job payloads. When jobs are retrieved for execution, their conditions must be
   * deserialized back into objects for evaluation. The serialVersionUID ensures that stored
   * conditions can be properly deserialized even after code changes.
   *
   * <p>If the class structure changes in an incompatible way, this value should be updated to
   * prevent deserialization of old, incompatible condition data.
   */
  @Serial private static final long serialVersionUID = -6905745576977735975L;

  /**
   * Creates a workflow condition with default priority (0).
   *
   * <p>This constructor is a convenience method for creating conditions without specifying a custom
   * priority. The default priority of 0 means this condition will be evaluated alongside other
   * default-priority conditions in definition order.
   *
   * @param type the condition type determining the evaluation strategy
   * @param expression the type-specific expression data (may be null for simple conditions)
   */
  public WorkflowCondition(ConditionType type, Serializable expression) {
    this(type, expression, 0);
  }

  /**
   * Creates a custom batch condition based on BatchContext evaluation.
   *
   * <p>The predicate receives the BatchContext with current batch state, allowing complex
   * conditions based on progress, timing, and custom metrics.
   *
   * @param predicate the batch condition evaluator
   * @return a BATCH_CUSTOM condition
   */
  public static WorkflowCondition batchCustom(SerializablePredicate<BatchContext> predicate) {
    return new WorkflowCondition(ConditionType.BATCH_CUSTOM, predicate);
  }

  // Static factory methods for common conditions

  /**
   * Creates a custom batch condition with specified priority.
   *
   * <p>Priority determines evaluation order when multiple batch conditions might match. Lower
   * values are evaluated first.
   *
   * @param predicate the batch condition evaluator
   * @param priority the evaluation priority (lower = higher priority)
   * @return a BATCH_CUSTOM condition with priority
   */
  public static WorkflowCondition batchCustom(
      SerializablePredicate<BatchContext> predicate, int priority) {
    return new WorkflowCondition(ConditionType.BATCH_CUSTOM, predicate, priority);
  }

  /**
   * Creates a condition for batch failure (one or more child jobs failed).
   *
   * <p>This condition evaluates to true when at least one child job in the batch fails.
   *
   * @return a BATCH_FAILURE condition
   */
  public static WorkflowCondition batchFailure() {
    return new WorkflowCondition(ConditionType.BATCH_FAILURE, null);
  }

  /**
   * Creates a condition for batch success (all child jobs completed successfully).
   *
   * <p>This condition evaluates to true only when every child job in the batch completes without
   * errors.
   *
   * @return a BATCH_SUCCESS condition
   */
  public static WorkflowCondition batchSuccess() {
    return new WorkflowCondition(ConditionType.BATCH_SUCCESS, null);
  }

  /**
   * Creates a custom condition based on complete JobResult evaluation.
   *
   * <p>The predicate receives the full JobResult object, allowing complex conditions based on
   * success status, return values, execution time, error details, and metadata.
   *
   * @param <T> the type of the job's return value
   * @param predicate the condition evaluator
   * @return a CUSTOM condition
   */
  public static <T> WorkflowCondition custom(SerializablePredicate<JobResult<T>> predicate) {
    return new WorkflowCondition(ConditionType.CUSTOM, predicate);
  }

  /**
   * Creates a custom condition with specified priority.
   *
   * <p>Priority determines evaluation order when multiple conditions might match. Lower values are
   * evaluated first.
   *
   * @param <T> the type of the job's return value
   * @param predicate the condition evaluator
   * @param priority the evaluation priority (lower = higher priority)
   * @return a CUSTOM condition with priority
   */
  public static <T> WorkflowCondition custom(
      SerializablePredicate<JobResult<T>> predicate, int priority) {
    return new WorkflowCondition(ConditionType.CUSTOM, predicate, priority);
  }

  /**
   * Creates a condition that triggers when a job fails.
   *
   * <p>This condition evaluates to true when the parent job throws an exception or otherwise fails
   * to complete normally.
   *
   * @return a FAILURE condition
   */
  public static WorkflowCondition failure() {
    return new WorkflowCondition(ConditionType.FAILURE, null);
  }

  /**
   * Creates a condition based on maximum acceptable batch failures.
   *
   * <p>The condition evaluates to true when the number of failed jobs is less than or equal to the
   * specified maximum.
   *
   * @param maxFailures maximum number of failures allowed
   * @return a BATCH_FAILURE_COUNT condition
   * @throws IllegalArgumentException if maxFailures is negative
   */
  public static WorkflowCondition failureCount(int maxFailures) {
    if (maxFailures < 0) {
      throw new IllegalArgumentException("Failure count must be non-negative");
    }
    return new WorkflowCondition(ConditionType.BATCH_FAILURE_COUNT, maxFailures);
  }

  /**
   * Creates a condition based on the job's return value.
   *
   * <p>The function receives only the return value (not the full JobResult) and should return true
   * to trigger the branch. This is a convenience method for value-based conditions.
   *
   * @param <T> the type of the job's return value
   * @param function transforms the return value to a boolean decision
   * @return a RESULT_VALUE condition
   */
  public static <T> WorkflowCondition result(SerializableFunction<T, Boolean> function) {
    return new WorkflowCondition(ConditionType.RESULT_VALUE, function);
  }

  /**
   * Creates a condition that triggers when a job completes successfully.
   *
   * <p>This condition evaluates to true when the parent job finishes without throwing an exception.
   *
   * @return a SUCCESS condition
   */
  public static WorkflowCondition success() {
    return new WorkflowCondition(ConditionType.SUCCESS, null);
  }

  /**
   * Creates a condition based on batch success rate threshold.
   *
   * <p>The condition evaluates to true when the percentage of successful jobs meets or exceeds the
   * specified threshold.
   *
   * @param minRate minimum success rate required (0.0 to 1.0)
   * @return a BATCH_SUCCESS_RATE condition
   * @throws IllegalArgumentException if minRate is not between 0.0 and 1.0
   */
  public static WorkflowCondition successRate(double minRate) {
    if (minRate < 0.0 || minRate > 1.0) {
      throw new IllegalArgumentException("Success rate must be between 0.0 and 1.0");
    }
    return new WorkflowCondition(ConditionType.BATCH_SUCCESS_RATE, minRate);
  }

  /**
   * Enumeration of supported workflow condition types.
   *
   * <p>Each type represents a different evaluation strategy for determining whether a workflow
   * branch should execute.
   */
  public enum ConditionType {
    /** Job completed successfully without exceptions */
    SUCCESS,

    /** Job failed with an exception or error status */
    FAILURE,

    /** Custom condition evaluated using a predicate on JobResult */
    CUSTOM,

    /** Condition evaluated based on the job's return value */
    RESULT_VALUE,

    /** Batch completed with all child jobs successful */
    BATCH_SUCCESS,

    /** Batch completed with one or more failed child jobs */
    BATCH_FAILURE,

    /** Batch success rate meets or exceeds specified threshold */
    BATCH_SUCCESS_RATE,

    /** Number of batch failures is within acceptable threshold */
    BATCH_FAILURE_COUNT,

    /** Custom batch condition evaluated using a predicate on BatchContext */
    BATCH_CUSTOM
  }
}
