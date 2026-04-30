package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;
import java.time.Instant;
import java.util.UUID;
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

  @Test
  void findConditionsByChildJobId_returnsCorrectConditions() {
    var parent1 = persist(newPendingJob());
    var parent2 = persist(newPendingJob());
    var child = persist(newPendingJob());

    store().saveCondition(newCondition(parent1.getId(), child.getId()));
    store().saveCondition(newCondition(parent2.getId(), child.getId()));

    var conditions = store().findConditionsByChildJobId(child.getId());

    assertEquals(2, conditions.size(), "findConditionsByChildJobId should return both conditions");
  }

  @Test
  void findConditionsByType_filtersCorrectly() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    WorkflowConditionEntity success = newCondition(parent.getId(), childA.getId());
    success.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    store().saveCondition(success);

    WorkflowConditionEntity failure = newCondition(parent.getId(), childB.getId());
    failure.setConditionType(WorkflowCondition.ConditionType.FAILURE);
    store().saveCondition(failure);

    var successConditions =
        store().findConditionsByType(parent.getId(), WorkflowCondition.ConditionType.SUCCESS);

    assertEquals(1, successConditions.size(), "Should return only SUCCESS conditions");
    assertEquals(childA.getId(), successConditions.get(0).getChildJobId());
  }

  @Test
  void deleteConditionById_removesSingleCondition() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    var saved1 = store().saveCondition(newCondition(parent.getId(), childA.getId()));
    store().saveCondition(newCondition(parent.getId(), childB.getId()));

    store().deleteConditionById(saved1.getId());

    assertEquals(
        1,
        store().countConditionsByParentJobId(parent.getId()),
        "Only one condition should remain after deleting the other");
  }

  @Test
  void deleteConditionsByChildJobId_removesAll() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    store().saveCondition(newCondition(parent.getId(), child.getId()));

    store().deleteConditionsByChildJobId(child.getId());

    assertTrue(
        store().findConditionsByChildJobId(child.getId()).isEmpty(),
        "All conditions for the child should be deleted");
  }

  @Test
  void countConditionsByParentJobId_returnsAccurateCount() {
    var parent = persist(newPendingJob());

    store().saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));
    store().saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));
    store().saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));

    long count = store().countConditionsByParentJobId(parent.getId());

    assertEquals(3, count, "countConditionsByParentJobId should return 3");
  }

  @Test
  void findConditionById_unknownId_returnsNull() {
    var result = store().findConditionById(new UUID(0L, Long.MAX_VALUE));

    assertNull(result, "findConditionById with unknown ID should return null");
  }

  private WorkflowConditionEntity newCondition(UUID parentJobId, UUID childJobId) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parentJobId);
    condition.setChildJobId(childJobId);
    condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    condition.setConditionPriority(0);
    condition.setCreatedAt(Instant.now());
    return condition;
  }
}
