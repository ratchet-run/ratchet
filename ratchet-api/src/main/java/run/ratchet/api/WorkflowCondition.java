package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * Decision criteria for workflow branching after job execution. Conditions are evaluated in {@code
 * priority} order (lower = first); the first match wins.
 *
 * <pre>{@code
 * WorkflowCondition onSuccess  = WorkflowCondition.success();
 * WorkflowCondition highScore  = WorkflowCondition.result(score -> score > 90);
 * WorkflowCondition goodBatch  = WorkflowCondition.successRate(0.95);
 * }</pre>
 *
 * @param type the condition type (evaluation strategy)
 * @param expression type-specific expression data (predicate, threshold, or null)
 * @param priority evaluation order when multiple conditions match (lower = first, default 0)
 * @see JobBuilder#when(SerializablePredicate, SerializableCheckedRunnable)
 * @see BatchBuilder#thenWhenBatch(SerializablePredicate, SerializableCheckedRunnable)
 * @see WorkflowBranch
 */
public record WorkflowCondition(ConditionType type, Serializable expression, int priority)
    implements Serializable {

  @Serial private static final long serialVersionUID = -6905745576977735975L;

  /** Creates a condition with default priority (0). */
  public WorkflowCondition(ConditionType type, Serializable expression) {
    this(type, expression, 0);
  }

  /** Creates a BATCH_CUSTOM condition evaluated against the current {@link BatchContext}. */
  public static WorkflowCondition batchCustom(SerializablePredicate<BatchContext> predicate) {
    return new WorkflowCondition(ConditionType.BATCH_CUSTOM, predicate);
  }

  /** Creates a BATCH_CUSTOM condition with explicit evaluation priority (lower = first). */
  public static WorkflowCondition batchCustom(
      SerializablePredicate<BatchContext> predicate, int priority) {
    return new WorkflowCondition(ConditionType.BATCH_CUSTOM, predicate, priority);
  }

  /** Creates a BATCH_FAILURE condition (true when at least one child job fails). */
  public static WorkflowCondition batchFailure() {
    return new WorkflowCondition(ConditionType.BATCH_FAILURE, null);
  }

  /** Creates a BATCH_SUCCESS condition (true when every child job completes without error). */
  public static WorkflowCondition batchSuccess() {
    return new WorkflowCondition(ConditionType.BATCH_SUCCESS, null);
  }

  /** Creates a CUSTOM condition evaluated against the full {@link JobResult}. */
  public static <T> WorkflowCondition custom(SerializablePredicate<JobResult<T>> predicate) {
    return new WorkflowCondition(ConditionType.CUSTOM, predicate);
  }

  /** Creates a CUSTOM condition with explicit evaluation priority (lower = first). */
  public static <T> WorkflowCondition custom(
      SerializablePredicate<JobResult<T>> predicate, int priority) {
    return new WorkflowCondition(ConditionType.CUSTOM, predicate, priority);
  }

  /** Creates a FAILURE condition (true when the job throws or fails to complete normally). */
  public static WorkflowCondition failure() {
    return new WorkflowCondition(ConditionType.FAILURE, null);
  }

  /**
   * Creates a BATCH_FAILURE_COUNT condition (true when failures &lt;= maxFailures).
   *
   * @throws IllegalArgumentException if maxFailures is negative
   */
  public static WorkflowCondition failureCount(int maxFailures) {
    if (maxFailures < 0) {
      throw new IllegalArgumentException("Failure count must be non-negative");
    }
    return new WorkflowCondition(ConditionType.BATCH_FAILURE_COUNT, maxFailures);
  }

  /** Creates a RESULT_VALUE condition evaluated against the job's return value only. */
  public static <T> WorkflowCondition result(SerializableFunction<T, Boolean> function) {
    return new WorkflowCondition(ConditionType.RESULT_VALUE, function);
  }

  /** Creates a SUCCESS condition (true when the job finishes without throwing). */
  public static WorkflowCondition success() {
    return new WorkflowCondition(ConditionType.SUCCESS, null);
  }

  /**
   * Creates a BATCH_SUCCESS_RATE condition (true when success rate &gt;= minRate).
   *
   * @param minRate must be between 0.0 and 1.0
   * @throws IllegalArgumentException if minRate is out of range
   */
  public static WorkflowCondition successRate(double minRate) {
    if (minRate < 0.0 || minRate > 1.0) {
      throw new IllegalArgumentException("Success rate must be between 0.0 and 1.0");
    }
    return new WorkflowCondition(ConditionType.BATCH_SUCCESS_RATE, minRate);
  }

  /** Evaluation strategy for a workflow condition. */
  public enum ConditionType {
    SUCCESS,
    FAILURE,
    CUSTOM,
    RESULT_VALUE,
    BATCH_SUCCESS,
    BATCH_FAILURE,
    BATCH_SUCCESS_RATE,
    BATCH_FAILURE_COUNT,
    BATCH_CUSTOM
  }
}
