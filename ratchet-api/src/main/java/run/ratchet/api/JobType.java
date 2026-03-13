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
  /**
   * Standard one-time execution job scheduled for a specific time.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Executes exactly once at the scheduled time
   *   <li>No automatic rescheduling after completion
   *   <li>Most common job type for ad-hoc tasks
   *   <li>Supports all standard features: retries, timeouts, dependencies
   *   <li>Created via {@code JobScheduler.schedule()} methods
   * </ul>
   */
  SINGLE,

  /**
   * Automatically rescheduled job based on cron expression or fixed intervals.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Automatically creates next instance after completion
   *   <li>Uses cron expressions (e.g., "0 0 * * *" for daily)
   *   <li>Next execution time calculated from configured timezone
   *   <li>Continues until explicitly stopped or end date reached
   *   <li>Created via {@code RecurringScheduler} or cron methods
   *   <li>Indexed separately for efficient recurring job queries
   * </ul>
   */
  RECURRING,

  /**
   * Batch-style work consisting of multiple coordinated child executions.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Created through batch builders
   *   <li>May execute many individual items behind the scenes
   *   <li>Reports progress and completion at the batch level
   * </ul>
   */
  BATCH,

  /**
   * Sequential multi-step work such as pipelines or chained tasks.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Created through chaining APIs
   *   <li>Represents ordered step-by-step processing
   *   <li>May be executed internally as multiple linked jobs
   * </ul>
   */
  CHAIN,

  /**
   * Workflow-driven execution using conditional branches or future join semantics.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Created through workflow/conditional APIs
   *   <li>Can react to prior results or batch outcomes
   *   <li>May be implemented with internal branch or join jobs
   * </ul>
   */
  WORKFLOW,

  /**
   * Scheduler-managed system work not directly created by users.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Reserved for framework-owned execution paths
   *   <li>Not user-creatable through the public scheduling API
   * </ul>
   */
  SYSTEM
}
