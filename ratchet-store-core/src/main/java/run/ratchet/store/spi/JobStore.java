package run.ratchet.store.spi;

/**
 * Composed store abstraction for all job persistence operations.
 *
 * <p>Extends focused sub-interfaces to provide a single type that implementors satisfy. The
 * sub-interface decomposition enables future TCK modularity and reduces cognitive load.
 *
 * <p>Implementations must be thread-safe.
 *
 * @see JobCrudStore
 * @see JobClaimStore
 * @see JobStatusStore
 * @see JobBulkStore
 * @see BatchStore
 * @see LockStore
 * @see NodeStore
 * @see ArchiveStore
 * @see ExecutionStore
 * @see JobLogStore
 * @see TagStore
 * @see WorkflowConditionStore
 * @see BatchMetricsStore
 * @see DlqAlertStore
 * @see ResourcePermitStore
 */
public interface JobStore
    extends JobCrudStore,
        JobClaimStore,
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
