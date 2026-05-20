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
        SignalStore {
  // RecurringJobStore is composed here once the legacy recurring methods are removed from
  // JobClaimStore / JobPauseStore / JobBatchStatusStore / JobCrudStore. Until then, store
  // impls implement RecurringJobStore directly as a sibling interface.
  // Marker interface — all methods inherited from sub-interfaces
}
