package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code WorkflowConditionStore}. */
public abstract class AbstractWorkflowConditionStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupWorkflowConditionFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindCondition_roundTrips() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parent.getId());
    condition.setChildJobId(child.getId());
    condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    condition.setConditionPriority(0);
    condition.setCreatedAt(Instant.now());

    var saved = store().saveCondition(condition);
    assertNotNull(saved.getId(), "Saved condition should have an assigned ID");

    var reloaded = store().findConditionById(saved.getId());
    assertNotNull(reloaded, "findConditionById should return the persisted condition");
    assertEquals(saved.getId(), reloaded.getId());
    assertEquals(parent.getId(), reloaded.getParentJobId());
    assertEquals(child.getId(), reloaded.getChildJobId());
  }

  @Test
  void findConditionsByParentJobId_returnsAllConditions() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    WorkflowConditionEntity first = new WorkflowConditionEntity();
    first.setParentJobId(parent.getId());
    first.setChildJobId(childA.getId());
    first.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    first.setConditionPriority(0);
    first.setCreatedAt(Instant.now());
    store().saveCondition(first);

    WorkflowConditionEntity second = new WorkflowConditionEntity();
    second.setParentJobId(parent.getId());
    second.setChildJobId(childB.getId());
    second.setConditionType(WorkflowCondition.ConditionType.FAILURE);
    second.setConditionPriority(1);
    second.setCreatedAt(Instant.now());
    store().saveCondition(second);

    var conditions = store().findConditionsByParentJobId(parent.getId());
    assertEquals(2, conditions.size(), "findConditionsByParentJobId should return both conditions");
  }

  @Test
  void deleteConditionsByParentJobId_removesAll() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parent.getId());
    condition.setChildJobId(child.getId());
    condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    condition.setConditionPriority(0);
    condition.setCreatedAt(Instant.now());
    store().saveCondition(condition);

    store().deleteConditionsByParentJobId(parent.getId());

    assertEquals(
        0,
        store().countConditionsByParentJobId(parent.getId()),
        "All conditions for the parent should be deleted");
  }
}
