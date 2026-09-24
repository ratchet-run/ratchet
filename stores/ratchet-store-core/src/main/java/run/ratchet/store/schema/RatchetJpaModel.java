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
package run.ratchet.store.schema;

import java.util.List;

/** Managed class names without loading entities before provider class transformation. */
public final class RatchetJpaModel {
  private RatchetJpaModel() {}

  public static final List<String> ENTITY_CLASS_NAMES =
      List.of(
          "run.ratchet.store.entity.ArchivedJobEntity",
          "run.ratchet.store.entity.BatchEntity",
          "run.ratchet.store.entity.BatchMetricsEntity",
          "run.ratchet.store.entity.JobEntity",
          "run.ratchet.store.entity.JobExecutionEntity",
          "run.ratchet.store.entity.JobLogEntity",
          "run.ratchet.store.entity.NodeEntity",
          "run.ratchet.store.entity.ResourceLimitEntity",
          "run.ratchet.store.entity.ResourcePermitEntity",
          "run.ratchet.store.entity.WorkflowConditionEntity");

  public static final List<String> CONVERTER_CLASS_NAMES =
      List.of(
          "run.ratchet.store.converter.JobPayloadConverter",
          "run.ratchet.store.converter.JobPriorityConverter",
          "run.ratchet.store.converter.JsonMapConverter",
          "run.ratchet.store.converter.JsonObjectMapConverter",
          "run.ratchet.store.converter.JsonListConverter");
}
