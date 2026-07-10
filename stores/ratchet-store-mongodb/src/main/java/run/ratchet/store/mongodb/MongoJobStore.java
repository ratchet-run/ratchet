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
package run.ratchet.store.mongodb;

import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Public MongoDB-specific store type.
 *
 * <p>The CDI implementation is package-private; consumers can inject this interface without
 * exposing the concrete store as an extension point.
 *
 * <p>The MongoDB store advertises every optional capability, so this type extends the mandatory
 * {@link JobStore} core plus all capability interfaces. Listing them explicitly (rather than
 * relying on transitive inheritance) keeps each capability in the bean's CDI type closure so {@code
 * Instance<CapabilityType>} resolves against this store.
 */
public interface MongoJobStore
    extends JobStore,
        RecurringJobStore,
        BatchStore,
        WorkflowConditionStore,
        SignalStore,
        ResourcePermitStore,
        LockStore,
        ArchiveStore,
        JobQueryStore,
        JobAnalyticsStore,
        JobAuditStore,
        DlqAlertStore,
        JobExtensionStore {}
