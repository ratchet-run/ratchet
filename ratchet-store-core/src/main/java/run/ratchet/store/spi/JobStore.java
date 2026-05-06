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
  // Marker interface — all methods inherited from sub-interfaces
}
