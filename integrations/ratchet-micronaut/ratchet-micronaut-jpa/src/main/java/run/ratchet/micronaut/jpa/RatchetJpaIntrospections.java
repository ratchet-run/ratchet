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
package run.ratchet.micronaut.jpa;

import io.micronaut.core.annotation.Introspected;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;

/**
 * Generates bean introspections for Ratchet's pre-built entities. Micronaut Hibernate validates
 * entities on persist through Micronaut Validation, which rejects any class without an
 * introspection; Ratchet's entities are compiled without Micronaut, so they need this.
 */
@Introspected(
    classes = {
      ArchivedJobEntity.class,
      BatchEntity.class,
      BatchMetricsEntity.class,
      JobEntity.class,
      JobExecutionEntity.class,
      JobLogEntity.class,
      NodeEntity.class,
      ResourceLimitEntity.class,
      ResourcePermitEntity.class,
      WorkflowConditionEntity.class
    })
final class RatchetJpaIntrospections {
  private RatchetJpaIntrospections() {}
}
