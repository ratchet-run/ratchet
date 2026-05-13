package run.ratchet.api;

/** Public job categories exposed in events and SPIs. */
public enum JobType {
  /** A one-shot background task executed once and not rescheduled. */
  SINGLE,

  /** Automatically rescheduled job based on cron expression or fixed interval. */
  RECURRING,

  /** A batch-processing job comprising multiple child items processed in parallel. */
  BATCH,

  /** A sequenced chain of tasks executed in order, one after the other. */
  CHAIN,

  /** Workflow-driven execution using conditional branches or join semantics. */
  WORKFLOW,

  /** Scheduler-managed system work, not user-creatable. */
  SYSTEM
}
