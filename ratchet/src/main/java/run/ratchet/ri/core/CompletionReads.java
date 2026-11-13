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
package run.ratchet.ri.core;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/** Internal, explicitly selected read view for completion planning. */
public interface CompletionReads {
  Optional<JobEntity> findById(UUID id);

  List<JobEntity> findByIds(List<UUID> ids);

  List<JobEntity> findDependants(UUID parentId, int limit, int offset);

  List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentId);

  static CompletionReads plain(JobCrudStore jobs, WorkflowConditionStore conditions) {
    return new CompletionReads() {
      public Optional<JobEntity> findById(UUID id) {
        return jobs.findById(id);
      }

      public List<JobEntity> findByIds(List<UUID> ids) {
        return jobs.findByIds(ids);
      }

      public List<JobEntity> findDependants(UUID id, int limit, int offset) {
        return jobs.findDependants(id, limit, offset);
      }

      public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID id) {
        return conditions == null ? List.of() : conditions.findConditionsByParentJobId(id);
      }
    };
  }

  static CompletionReads transactional(TransactionalCompletionStore store) {
    return new CompletionReads() {
      public Optional<JobEntity> findById(UUID id) {
        return store.findByIdForCompletion(id);
      }

      public List<JobEntity> findByIds(List<UUID> ids) {
        return store.findByIdsForCompletion(ids);
      }

      public List<JobEntity> findDependants(UUID id, int limit, int offset) {
        return store.findDependantsForCompletion(id, limit, offset);
      }

      public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID id) {
        return store.findConditionsForCompletion(id);
      }
    };
  }
}
