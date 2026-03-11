package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents a conditional branch in a job workflow execution path.
 *
 * <p>WorkflowBranch encapsulates a condition-action pair that defines when and what additional job
 * should be executed based on the outcome of a parent job or batch. This enables sophisticated
 * workflow patterns including error handling, success paths, performance-based decisions, and
 * complex business logic flows.
 *
 * <h2>Key Components:</h2>
 *
 * <ul>
 *   <li><b>Condition</b> - Determines when this branch should execute
 *   <li><b>Task</b> - The serializable task to execute if the condition is met
 *   <li><b>Description</b> - Optional human-readable description for monitoring
 *   <li><b>Priority</b> - Execution order when multiple branches match
 * </ul>
 *
 * <h2>Workflow Patterns:</h2>
 *
 * <p>WorkflowBranch enables various patterns:
 *
 * <ul>
 *   <li><b>Success/Failure Paths</b> - Different actions based on job outcome
 *   <li><b>Performance Monitoring</b> - Actions triggered by execution metrics
 *   <li><b>Business Rules</b> - Complex conditions based on job results
 *   <li><b>Cascading Workflows</b> - Multi-stage processing pipelines
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // Created internally by JobBuilder/BatchBuilder
 * WorkflowBranch successBranch = new WorkflowBranch(
 *     WorkflowCondition.success(),
 *     (Serializable & Runnable) () -> sendSuccessNotification(),
 *     "Send notification on successful completion"
 * );
 *
 * // Using factory methods
 * WorkflowBranch branch = WorkflowBranch.of(
 *     WorkflowCondition.batchSuccessRate(0.95),
 *     (Serializable & Runnable) () -> processBatchResults(),
 *     "Process results when 95% success rate achieved"
 * );
 * }</pre>
 *
 * <h2>Execution Order:</h2>
 *
 * <p>When multiple branches have conditions that evaluate to true, they are executed based on their
 * priority (lower values first). Branches with the same priority execute in the order they were
 * defined.
 *
 * @param condition The condition that determines if this branch should execute.
 *     <p>This field holds the evaluation criteria (e.g., success, failure, custom predicate) that
 *     must be satisfied for the associated task to be enqueued. The condition is evaluated after
 *     the parent job completes, using the parent's result context.
 * @param task The serializable task to execute if the condition is met.
 *     <p>This field contains the serialized job definition that will be enqueued when the condition
 *     evaluates to true. The task includes the job's work unit and any nested workflow branches.
 * @param description Optional human-readable description for debugging and monitoring purposes.
 *     <p>This field provides context about the branch's purpose and is displayed in logs,
 *     monitoring dashboards, and admin interfaces. While optional, providing a meaningful
 *     description significantly improves workflow observability and troubleshooting.
 * @see WorkflowCondition
 * @see JobBuilder#branch(WorkflowCondition, SerializableCheckedRunnable, String)
 * @see BatchBuilder#thenBranch(WorkflowCondition, SerializableCheckedRunnable, String)
 */
public record WorkflowBranch(WorkflowCondition condition, Serializable task, String description)
    implements Serializable {

  /**
   * Serialization version identifier for ensuring compatibility during deserialization.
   *
   * <p>This field is required because WorkflowBranch instances are persisted as part of job
   * payloads in the database. When jobs are retrieved for execution, they must be deserialized back
   * into objects. The serialVersionUID ensures that stored branches can be properly deserialized
   * even after code changes, as long as compatibility is maintained.
   *
   * <p>If the class structure changes in an incompatible way, this value should be updated to
   * prevent deserialization of old, incompatible branch data.
   */
  @Serial private static final long serialVersionUID = -5529141024148855247L;

  /**
   * Creates a workflow branch without a description.
   *
   * <p>This constructor is a convenience method for creating simple branches where the condition
   * and task are self-explanatory. The description will be set to null.
   *
   * @param condition the condition that triggers this branch
   * @param task the serializable task to execute when the condition is met
   */
  public WorkflowBranch(WorkflowCondition condition, Serializable task) {
    this(condition, task, null);
  }

  /**
   * Creates a workflow branch with a descriptive label.
   *
   * <p>The description helps with monitoring, debugging, and understanding the workflow logic. It
   * appears in logs and monitoring dashboards.
   *
   * @param condition the condition that triggers this branch
   * @param task the serializable task to execute when the condition is met
   * @param description human-readable description of this branch's purpose
   * @return a new WorkflowBranch instance
   */
  public static WorkflowBranch of(
      WorkflowCondition condition, Serializable task, String description) {
    return new WorkflowBranch(condition, task, description);
  }

  /**
   * Creates a workflow branch without a description.
   *
   * <p>Use this factory method for simple branches where the condition and task are
   * self-explanatory.
   *
   * @param condition the condition that triggers this branch
   * @param task the serializable task to execute when the condition is met
   * @return a new WorkflowBranch instance
   */
  public static WorkflowBranch of(WorkflowCondition condition, Serializable task) {
    return new WorkflowBranch(condition, task);
  }

  /**
   * Gets the execution priority from the underlying condition.
   *
   * <p>Priority determines the order in which branches are evaluated and executed when multiple
   * conditions match. Lower priority values are executed first. The default priority is 0.
   *
   * @return the priority value from the condition
   */
  public int getPriority() {
    return condition.priority();
  }
}
