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

  WorkflowConditionEntity findConditionById(UUID id);

  List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId);

  List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId);

  List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type);

  void deleteConditionById(UUID id);

  void deleteConditionsByParentJobId(UUID parentJobId);

  void deleteConditionsByChildJobId(UUID childJobId);

  long countConditionsByParentJobId(UUID parentJobId);
}
