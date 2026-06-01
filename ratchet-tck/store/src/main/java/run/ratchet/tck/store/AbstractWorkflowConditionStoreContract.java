/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.WorkflowConditionEntity;

/** Base contract tests for {@code WorkflowConditionStore}. */
public abstract class AbstractWorkflowConditionStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupWorkflowConditionFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindCondition_roundTrips() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    WorkflowConditionEntity condition = newCondition(parent.getId(), child.getId());

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

    WorkflowConditionEntity first = newCondition(parent.getId(), childA.getId());
    store().saveCondition(first);

    WorkflowConditionEntity second =
        newCondition(parent.getId(), childB.getId(), WorkflowCondition.ConditionType.FAILURE, 1);
    store().saveCondition(second);

    var conditions = store().findConditionsByParentJobId(parent.getId());
    assertEquals(2, conditions.size(), "findConditionsByParentJobId should return both conditions");
  }

  @Test
  void findConditionsByParentJobId_ordersByConditionPriority() {
    var parent = persist(newPendingJob());
    var lowPriorityChild = persist(newPendingJob());
    var highPriorityChild = persist(newPendingJob());
    var middlePriorityChild = persist(newPendingJob());

    store()
        .saveCondition(
            newCondition(
                parent.getId(),
                lowPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    store()
        .saveCondition(
            newCondition(
                parent.getId(),
                highPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));
    store()
        .saveCondition(
            newCondition(
                parent.getId(),
                middlePriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                10));

    var orderedChildIds =
        store().findConditionsByParentJobId(parent.getId()).stream()
            .map(WorkflowConditionEntity::getChildJobId)
            .toList();

    assertEquals(
        List.of(highPriorityChild.getId(), middlePriorityChild.getId(), lowPriorityChild.getId()),
        orderedChildIds,
        "parent condition scans should evaluate lower priority numbers first");
  }

  @Test
  void deleteConditionsByParentJobId_removesAll() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    WorkflowConditionEntity condition = newCondition(parent.getId(), child.getId());
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
  void findConditionsByChildJobId_ordersByConditionPriority() {
    var highPriorityParent = persist(newPendingJob());
    var lowPriorityParent = persist(newPendingJob());
    var child = persist(newPendingJob());

    store()
        .saveCondition(
            newCondition(
                lowPriorityParent.getId(),
                child.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    store()
        .saveCondition(
            newCondition(
                highPriorityParent.getId(),
                child.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));

    var orderedParentIds =
        store().findConditionsByChildJobId(child.getId()).stream()
            .map(WorkflowConditionEntity::getParentJobId)
            .toList();

    assertEquals(
        List.of(highPriorityParent.getId(), lowPriorityParent.getId()),
        orderedParentIds,
        "child condition scans should evaluate lower priority numbers first");
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
  void findConditionsByType_ordersByConditionPriority() {
    var parent = persist(newPendingJob());
    var lowPriorityChild = persist(newPendingJob());
    var highPriorityChild = persist(newPendingJob());

    store()
        .saveCondition(
            newCondition(
                parent.getId(),
                lowPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    store()
        .saveCondition(
            newCondition(
                parent.getId(),
                highPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));

    var orderedChildIds =
        store()
            .findConditionsByType(parent.getId(), WorkflowCondition.ConditionType.SUCCESS)
            .stream()
            .map(WorkflowConditionEntity::getChildJobId)
            .toList();

    assertEquals(
        List.of(highPriorityChild.getId(), lowPriorityChild.getId()),
        orderedChildIds,
        "type condition scans should evaluate lower priority numbers first");
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
    return newCondition(parentJobId, childJobId, WorkflowCondition.ConditionType.SUCCESS, 0);
  }

  private WorkflowConditionEntity newCondition(
      UUID parentJobId,
      UUID childJobId,
      WorkflowCondition.ConditionType conditionType,
      int conditionPriority) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setParentJobId(parentJobId);
    condition.setChildJobId(childJobId);
    condition.setConditionType(conditionType);
    condition.setConditionPriority(conditionPriority);
    condition.setCreatedAt(Instant.now());
    return condition;
  }
}
