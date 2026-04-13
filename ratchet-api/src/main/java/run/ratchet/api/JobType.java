package run.ratchet.api;

/** Public job categories exposed in events and SPIs. */
public enum JobType {
  SINGLE,

  /** Automatically rescheduled job based on cron expression or fixed interval. */
  RECURRING,

  BATCH,

  CHAIN,

  /** Workflow-driven execution using conditional branches or join semantics. */
  WORKFLOW,

  /** Scheduler-managed system work, not user-creatable. */
  SYSTEM
}
