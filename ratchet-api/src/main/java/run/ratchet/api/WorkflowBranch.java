package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * A condition-action pair in a job workflow. When the condition evaluates to true after a parent
 * job completes, the associated task is enqueued. Multiple matching branches execute in priority
 * order (lower first).
 *
 * @param condition the condition that triggers this branch
 * @param task the serializable task to execute
 * @param description optional label for monitoring and debugging
 * @see WorkflowCondition
 * @see JobBuilder#branch(WorkflowCondition, SerializableCheckedRunnable, String)
 */
public record WorkflowBranch(WorkflowCondition condition, Serializable task, String description)
    implements Serializable {

  @Serial private static final long serialVersionUID = -5529141024148855247L;

  /** Creates a workflow branch without a description. */
  public WorkflowBranch(WorkflowCondition condition, Serializable task) {
    this(condition, task, null);
  }

  /**
   * @return the priority from the underlying condition (lower = first)
   */
  public int getPriority() {
    return condition.priority();
  }
}
