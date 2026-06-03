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

import java.util.Optional;
import run.ratchet.api.Incubating;

/**
 * Composed store abstraction for all job persistence operations. Implementations must be
 * thread-safe.
 */
@Incubating
public interface JobStore
    extends JobCrudStore,
        JobAnalyticsStore,
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
        JobAuditStore,
        TagStore,
        WorkflowConditionStore,
        DlqAlertStore,
        ResourcePermitStore,
        SignalStore,
        RecurringJobStore {

  /**
   * Returns this store's view as {@code type} when the store advertises that optional capability,
   * otherwise {@link Optional#empty()}.
   *
   * <p>This is the runtime probe seam for optional store capabilities. The reference engine calls
   * it to decide whether a capability-dependent feature (recurring scheduling, batch fan-out,
   * signals, archiving, …) is available, rather than assuming every store implements every
   * capability interface.
   *
   * <p>The default implementation reflects explicit Java type membership: a store advertises a
   * capability simply by implementing its interface. Implementations that override this method
   * <strong>must</strong> advertise a capability all-or-nothing — never report a capability whose
   * methods are partially or unsupported, since callers treat a present capability as fully usable.
   *
   * @param type the capability interface to probe for; never {@code null}
   * @param <T> the capability type
   * @return the store viewed as {@code type}, or empty when the capability is not advertised
   */
  default <T> Optional<T> capability(Class<T> type) {
    return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
  }
}
