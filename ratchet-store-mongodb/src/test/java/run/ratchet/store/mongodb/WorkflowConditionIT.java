package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for workflow condition (parent-child dependency) operations.
 *
 * <p>Validates creating conditions, querying by parent/child, and the priority ordering used by the
 * workflow engine.
 */
class WorkflowConditionIT extends BaseDocumentStoreIT {

  @Test
  void createAndQueryByParent() {
    JobEntity parent = store().save(newPendingJob());
    JobEntity child1 = store().save(newChainStepJob());
    JobEntity child2 = store().save(newChainStepJob());

    WorkflowConditionEntity cond1 = new WorkflowConditionEntity();
    cond1.setParentJobId(parent.getId());
    cond1.setChildJobId(child1.getId());
    cond1.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    cond1.setConditionPriority(1);
    store().saveCondition(cond1);

    WorkflowConditionEntity cond2 = new WorkflowConditionEntity();
    cond2.setParentJobId(parent.getId());
    cond2.setChildJobId(child2.getId());
    cond2.setConditionType(WorkflowCondition.ConditionType.FAILURE);
    cond2.setConditionPriority(2);
    store().saveCondition(cond2);

    List<WorkflowConditionEntity> conditions = store().findConditionsByParentJobId(parent.getId());
    assertEquals(2, conditions.size());
    // Should be ordered by priority
    assertEquals(1, conditions.get(0).getConditionPriority());
    assertEquals(2, conditions.get(1).getConditionPriority());
  }

  @Test
  void findConditionsByChildId() {
    JobEntity parent = store().save(newPendingJob());
    JobEntity child = store().save(newChainStepJob());

    WorkflowConditionEntity cond = new WorkflowConditionEntity();
    cond.setParentJobId(parent.getId());
    cond.setChildJobId(child.getId());
    cond.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    cond.setConditionPriority(1);
    store().saveCondition(cond);

    List<WorkflowConditionEntity> found = store().findConditionsByChildJobId(child.getId());
    assertEquals(1, found.size());
    assertEquals(parent.getId(), (long) found.get(0).getParentJobId());
  }

  @Test
  void deleteConditionsByParentId() {
    JobEntity parent = store().save(newPendingJob());
    JobEntity child = store().save(newChainStepJob());

    WorkflowConditionEntity cond = new WorkflowConditionEntity();
    cond.setParentJobId(parent.getId());
    cond.setChildJobId(child.getId());
    cond.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    cond.setConditionPriority(1);
    store().saveCondition(cond);

    store().deleteConditionsByParentJobId(parent.getId());
    assertTrue(store().findConditionsByParentJobId(parent.getId()).isEmpty());
  }
}
