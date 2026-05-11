package run.ratchet.store.spi;

import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;

/** Workflow condition persistence operations. */
@Incubating
public interface WorkflowConditionStore {

  WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition);

  /**
   * Finds a single workflow condition by id.
   *
   * @param id condition id
   * @return the condition, or {@code null} when no condition exists for the id
   */
  WorkflowConditionEntity findConditionById(UUID id);

  /**
   * Returns every condition for the parent job, ordered by store-defined workflow priority.
   * Workflow routing needs the complete condition set, so this is intentionally not paged; callers
   * should keep workflow graphs bounded at scheduling time.
   */
  List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId);

  /**
   * Returns every condition pointing at the child job. This is intentionally not paged because the
   * result represents the complete stored workflow graph for the child.
   */
  List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId);

  /**
   * Returns every condition of the requested type for the parent job. This is intentionally not
   * paged because workflow routing evaluates the complete matching set.
   */
  List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type);

  void deleteConditionById(UUID id);

  void deleteConditionsByParentJobId(UUID parentJobId);

  void deleteConditionsByChildJobId(UUID childJobId);

  long countConditionsByParentJobId(UUID parentJobId);
}
