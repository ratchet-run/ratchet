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
    condition.setDefinitionOrder(7);

    var saved = workflowConditionStore().saveCondition(condition);
    assertNotNull(saved.getId(), "Saved condition should have an assigned ID");

    var reloaded = workflowConditionStore().findConditionById(saved.getId());
    assertNotNull(reloaded, "findConditionById should return the persisted condition");
    assertEquals(saved.getId(), reloaded.getId());
    assertEquals(parent.getId(), reloaded.getParentJobId());
    assertEquals(child.getId(), reloaded.getChildJobId());
    assertEquals(7, reloaded.getDefinitionOrder());
  }

  @Test
  void findConditionsByParentJobId_returnsAllConditions() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    WorkflowConditionEntity first = newCondition(parent.getId(), childA.getId());
    workflowConditionStore().saveCondition(first);

    WorkflowConditionEntity second =
        newCondition(parent.getId(), childB.getId(), WorkflowCondition.ConditionType.FAILURE, 1);
    workflowConditionStore().saveCondition(second);

    var conditions = workflowConditionStore().findConditionsByParentJobId(parent.getId());
    assertEquals(2, conditions.size(), "findConditionsByParentJobId should return both conditions");
  }

  @Test
  void findConditionsByParentJobId_ordersByConditionPriority() {
    var parent = persist(newPendingJob());
    var lowPriorityChild = persist(newPendingJob());
    var highPriorityChild = persist(newPendingJob());
    var middlePriorityChild = persist(newPendingJob());

    workflowConditionStore()
        .saveCondition(
            newCondition(
                parent.getId(),
                lowPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    workflowConditionStore()
        .saveCondition(
            newCondition(
                parent.getId(),
                highPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));
    workflowConditionStore()
        .saveCondition(
            newCondition(
                parent.getId(),
                middlePriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                10));

    var orderedChildIds =
        workflowConditionStore().findConditionsByParentJobId(parent.getId()).stream()
            .map(WorkflowConditionEntity::getChildJobId)
            .toList();

    assertEquals(
        List.of(highPriorityChild.getId(), middlePriorityChild.getId(), lowPriorityChild.getId()),
        orderedChildIds,
        "parent condition scans should evaluate lower priority numbers first");
  }

  @Test
  void findConditionsByParentJobId_ordersEqualPriorityByDefinitionOrder() {
    var parent = persist(newPendingJob());
    var firstChild = persist(newPendingJob());
    var secondChild = persist(newPendingJob());
    var thirdChild = persist(newPendingJob());

    workflowConditionStore().saveCondition(newCondition(parent.getId(), thirdChild.getId(), 5, 2));
    workflowConditionStore().saveCondition(newCondition(parent.getId(), secondChild.getId(), 5, 1));
    workflowConditionStore().saveCondition(newCondition(parent.getId(), firstChild.getId(), 5, 0));

    var orderedChildIds =
        workflowConditionStore().findConditionsByParentJobId(parent.getId()).stream()
            .map(WorkflowConditionEntity::getChildJobId)
            .toList();

    assertEquals(
        List.of(firstChild.getId(), secondChild.getId(), thirdChild.getId()),
        orderedChildIds,
        "equal-priority parent conditions should follow workflow definition order");
  }

  @Test
  void legacyEqualPriorityConditions_useCanonicalIdAsStableFallback() {
    var parent = persist(newPendingJob());
    var firstChild = persist(newPendingJob());
    var secondChild = persist(newPendingJob());
    Instant sameTimestamp = Instant.parse("2026-07-12T00:00:00Z");
    UUID firstId = new UUID(0L, 1L);
    UUID secondId = new UUID(0L, 2L);

    WorkflowConditionEntity second = newCondition(parent.getId(), secondChild.getId());
    second.setId(secondId);
    second.setCreatedAt(sameTimestamp);
    workflowConditionStore().saveCondition(second);

    WorkflowConditionEntity first = newCondition(parent.getId(), firstChild.getId());
    first.setId(firstId);
    first.setCreatedAt(sameTimestamp);
    workflowConditionStore().saveCondition(first);

    var orderedConditionIds =
        workflowConditionStore().findConditionsByParentJobId(parent.getId()).stream()
            .map(WorkflowConditionEntity::getId)
            .toList();

    assertEquals(
        List.of(firstId, secondId),
        orderedConditionIds,
        "legacy definition_order=0 rows should use canonical UUID text as a stable fallback");
  }

  @Test
  void deleteConditionsByParentJobId_removesAll() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    WorkflowConditionEntity condition = newCondition(parent.getId(), child.getId());
    workflowConditionStore().saveCondition(condition);

    workflowConditionStore().deleteConditionsByParentJobId(parent.getId());

    assertEquals(
        0,
        workflowConditionStore().countConditionsByParentJobId(parent.getId()),
        "All conditions for the parent should be deleted");
  }

  @Test
  void findConditionsByChildJobId_returnsCorrectConditions() {
    var parent1 = persist(newPendingJob());
    var parent2 = persist(newPendingJob());
    var child = persist(newPendingJob());

    workflowConditionStore().saveCondition(newCondition(parent1.getId(), child.getId()));
    workflowConditionStore().saveCondition(newCondition(parent2.getId(), child.getId()));

    var conditions = workflowConditionStore().findConditionsByChildJobId(child.getId());

    assertEquals(2, conditions.size(), "findConditionsByChildJobId should return both conditions");
  }

  @Test
  void findConditionsByChildJobId_ordersByConditionPriority() {
    var highPriorityParent = persist(newPendingJob());
    var lowPriorityParent = persist(newPendingJob());
    var child = persist(newPendingJob());

    workflowConditionStore()
        .saveCondition(
            newCondition(
                lowPriorityParent.getId(),
                child.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    workflowConditionStore()
        .saveCondition(
            newCondition(
                highPriorityParent.getId(),
                child.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));

    var orderedParentIds =
        workflowConditionStore().findConditionsByChildJobId(child.getId()).stream()
            .map(WorkflowConditionEntity::getParentJobId)
            .toList();

    assertEquals(
        List.of(highPriorityParent.getId(), lowPriorityParent.getId()),
        orderedParentIds,
        "child condition scans should evaluate lower priority numbers first");
  }

  @Test
  void findConditionsByChildJobId_ordersEqualPriorityByDefinitionOrder() {
    var firstParent = persist(newPendingJob());
    var secondParent = persist(newPendingJob());
    var thirdParent = persist(newPendingJob());
    var child = persist(newPendingJob());

    workflowConditionStore().saveCondition(newCondition(thirdParent.getId(), child.getId(), 5, 2));
    workflowConditionStore().saveCondition(newCondition(secondParent.getId(), child.getId(), 5, 1));
    workflowConditionStore().saveCondition(newCondition(firstParent.getId(), child.getId(), 5, 0));

    var orderedParentIds =
        workflowConditionStore().findConditionsByChildJobId(child.getId()).stream()
            .map(WorkflowConditionEntity::getParentJobId)
            .toList();

    assertEquals(
        List.of(firstParent.getId(), secondParent.getId(), thirdParent.getId()),
        orderedParentIds,
        "equal-priority child condition scans should follow definition order");
  }

  @Test
  void findConditionsByType_filtersCorrectly() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    WorkflowConditionEntity success = newCondition(parent.getId(), childA.getId());
    success.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    workflowConditionStore().saveCondition(success);

    WorkflowConditionEntity failure = newCondition(parent.getId(), childB.getId());
    failure.setConditionType(WorkflowCondition.ConditionType.FAILURE);
    workflowConditionStore().saveCondition(failure);

    var successConditions =
        workflowConditionStore()
            .findConditionsByType(parent.getId(), WorkflowCondition.ConditionType.SUCCESS);

    assertEquals(1, successConditions.size(), "Should return only SUCCESS conditions");
    assertEquals(childA.getId(), successConditions.get(0).getChildJobId());
  }

  @Test
  void findConditionsByType_ordersByConditionPriority() {
    var parent = persist(newPendingJob());
    var lowPriorityChild = persist(newPendingJob());
    var highPriorityChild = persist(newPendingJob());

    workflowConditionStore()
        .saveCondition(
            newCondition(
                parent.getId(),
                lowPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                20));
    workflowConditionStore()
        .saveCondition(
            newCondition(
                parent.getId(),
                highPriorityChild.getId(),
                WorkflowCondition.ConditionType.SUCCESS,
                5));

    var orderedChildIds =
        workflowConditionStore()
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
  void findConditionsByType_ordersEqualPriorityByDefinitionOrder() {
    var parent = persist(newPendingJob());
    var firstChild = persist(newPendingJob());
    var secondChild = persist(newPendingJob());
    var thirdChild = persist(newPendingJob());

    workflowConditionStore().saveCondition(newCondition(parent.getId(), thirdChild.getId(), 5, 2));
    workflowConditionStore().saveCondition(newCondition(parent.getId(), secondChild.getId(), 5, 1));
    workflowConditionStore().saveCondition(newCondition(parent.getId(), firstChild.getId(), 5, 0));

    var orderedChildIds =
        workflowConditionStore()
            .findConditionsByType(parent.getId(), WorkflowCondition.ConditionType.SUCCESS)
            .stream()
            .map(WorkflowConditionEntity::getChildJobId)
            .toList();

    assertEquals(
        List.of(firstChild.getId(), secondChild.getId(), thirdChild.getId()),
        orderedChildIds,
        "equal-priority type condition scans should follow definition order");
  }

  @Test
  void deleteConditionById_removesSingleCondition() {
    var parent = persist(newPendingJob());
    var childA = persist(newPendingJob());
    var childB = persist(newPendingJob());

    var saved1 =
        workflowConditionStore().saveCondition(newCondition(parent.getId(), childA.getId()));
    workflowConditionStore().saveCondition(newCondition(parent.getId(), childB.getId()));

    workflowConditionStore().deleteConditionById(saved1.getId());

    assertEquals(
        1,
        workflowConditionStore().countConditionsByParentJobId(parent.getId()),
        "Only one condition should remain after deleting the other");
  }

  @Test
  void deleteConditionsByChildJobId_removesAll() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());

    workflowConditionStore().saveCondition(newCondition(parent.getId(), child.getId()));

    workflowConditionStore().deleteConditionsByChildJobId(child.getId());

    assertTrue(
        workflowConditionStore().findConditionsByChildJobId(child.getId()).isEmpty(),
        "All conditions for the child should be deleted");
  }

  @Test
  void countConditionsByParentJobId_returnsAccurateCount() {
    var parent = persist(newPendingJob());

    workflowConditionStore()
        .saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));
    workflowConditionStore()
        .saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));
    workflowConditionStore()
        .saveCondition(newCondition(parent.getId(), persist(newPendingJob()).getId()));

    long count = workflowConditionStore().countConditionsByParentJobId(parent.getId());

    assertEquals(3, count, "countConditionsByParentJobId should return 3");
  }

  @Test
  void findConditionById_unknownId_returnsNull() {
    var result = workflowConditionStore().findConditionById(new UUID(0L, Long.MAX_VALUE));

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

  private WorkflowConditionEntity newCondition(
      UUID parentJobId, UUID childJobId, int conditionPriority, int definitionOrder) {
    WorkflowConditionEntity condition =
        newCondition(
            parentJobId, childJobId, WorkflowCondition.ConditionType.SUCCESS, conditionPriority);
    condition.setDefinitionOrder(definitionOrder);
    return condition;
  }
}
