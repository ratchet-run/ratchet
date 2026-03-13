package run.ratchet.store.spi;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.util.List;

/** Workflow condition persistence operations. */
public interface WorkflowConditionStore {

  /** Creates or updates one workflow condition row. */
  WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition);

  /** Loads a workflow condition by primary key. */
  WorkflowConditionEntity findConditionById(long id);

  /** Lists all workflow conditions attached to a parent job. */
  List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId);

  /** Lists all workflow conditions referencing a child job. */
  List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId);

  /** Lists workflow conditions of a specific type for a parent job. */
  List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type);

  /** Deletes one workflow condition by primary key. */
  void deleteConditionById(long id);

  /** Deletes every workflow condition attached to a parent job. */
  void deleteConditionsByParentJobId(long parentJobId);

  /** Deletes every workflow condition referencing a child job. */
  void deleteConditionsByChildJobId(long childJobId);

  /** Counts workflow conditions attached to a parent job. */
  long countConditionsByParentJobId(long parentJobId);
}
