package run.ratchet.store.spi;

import run.ratchet.api.Incubating;

/**
 * Composed store abstraction for all job persistence operations. Implementations must be
 * thread-safe.
 */
@Incubating
@SuppressWarnings("deprecation") // JobStatusStore is a deprecated composed marker; kept here for
// one release so existing implementations that declare `implements JobStatusStore` directly still
// see the full method surface via the JobStore composition.
public interface JobStore
    extends JobCrudStore,
        JobClaimStore,
        JobTerminalStore,
        JobRetryStore,
        JobPauseStore,
        JobBatchStatusStore,
        JobStatusStore,
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
        ResourcePermitStore {
  // Marker interface — all methods inherited from sub-interfaces
}
