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
package run.ratchet.store.util;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import run.ratchet.store.entity.WorkflowConditionEntity;

/** Cross-store evaluation ordering for persisted workflow conditions. */
public final class WorkflowConditionOrdering {

  private static final Comparator<WorkflowConditionEntity> EVALUATION_ORDER =
      Comparator.comparingInt(
              (WorkflowConditionEntity condition) -> intOrZero(condition.getConditionPriority()))
          .thenComparingInt(condition -> intOrZero(condition.getDefinitionOrder()))
          .thenComparing(WorkflowConditionOrdering::canonicalId);

  private WorkflowConditionOrdering() {}

  /**
   * Orders conditions by priority, definition order, and a portable legacy fallback.
   *
   * <p>{@code definition_order = 0} is the schema default for rows created before definition order
   * was persisted. Their original registration order cannot be reconstructed, so canonical UUID
   * text provides a stable final tie-break independent of database UUID collation.
   */
  public static List<WorkflowConditionEntity> sorted(List<WorkflowConditionEntity> conditions) {
    return conditions.stream().sorted(EVALUATION_ORDER).toList();
  }

  private static int intOrZero(Integer value) {
    return value == null ? 0 : value;
  }

  private static String canonicalId(WorkflowConditionEntity condition) {
    UUID id = condition.getId();
    return id == null ? "" : id.toString();
  }
}
