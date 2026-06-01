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
package run.ratchet.store.spi;

import run.ratchet.api.Incubating;

/**
 * Composed store abstraction for all job persistence operations. Implementations must be
 * thread-safe.
 */
@Incubating
public interface JobStore
    extends JobCrudStore,
        JobQueryStore,
        JobClaimStore,
        JobTerminalStore,
        JobRetryStore,
        JobPauseStore,
        JobBatchStatusStore,
        JobBulkStore,
        BatchStore,
        LockStore,
        NodeStore,
        ArchiveStore,
        ExecutionStore,
        JobLogStore,
        TagStore,
        WorkflowConditionStore,
        BatchMetricsStore,
        DlqAlertStore,
        ResourcePermitStore,
        SignalStore,
        RecurringJobStore {
  // Marker interface — all methods inherited from sub-interfaces.
}
