package run.ratchet.api;

/**
 * Represents the public, high-level job categories exposed by the scheduling API.
 *
 * <p>These values intentionally describe the user-visible scheduling pattern rather than the
 * scheduler's internal execution mechanics. For example, a batch may be implemented internally
 * using parent and child jobs, but external observers still see it as a single {@link #BATCH}
 * category.
 *
 * <p>This enum is used in public events and SPIs where callers need a stable semantic category that
 * is portable across implementations.
 */
public enum JobType {
  /** Standard one-time execution job. */
  SINGLE,

  /** Automatically rescheduled job based on cron expression or fixed interval. */
  RECURRING,

  /** Batch-style work consisting of multiple coordinated child executions. */
  BATCH,

  /** Sequential multi-step work such as pipelines or chained tasks. */
  CHAIN,

  /** Workflow-driven execution using conditional branches or join semantics. */
  WORKFLOW,

  /** Scheduler-managed system work, not user-creatable. */
  SYSTEM
}
