package run.ratchet.store.spi;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.util.List;

/** Workflow condition persistence operations. */
public interface WorkflowConditionStore {

  WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition);

  WorkflowConditionEntity findConditionById(long id);

  List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId);

  List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId);

  List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type);

  void deleteConditionById(long id);

  void deleteConditionsByParentJobId(long parentJobId);

  void deleteConditionsByChildJobId(long childJobId);

  long countConditionsByParentJobId(long parentJobId);
}
