package run.ratchet.store.entity;

/**
 * Defines the lifecycle states of a job in the distributed scheduler system.
 *
 * <p>JobStatus represents the complete state machine for job execution, controlling how jobs
 * transition through the system from creation to completion. The status determines visibility to
 * pollers, retry eligibility, and archival behavior.
 *
 * <h2>State Transitions:</h2>
 *
 * <pre>
 *   PENDING -> RUNNING -> SUCCEEDED
 *              |          ^
 *            FAILED ------+ (with retries)
 *              |
 *           CANCELED (terminal)
 * </pre>
 *
 * @see JobEntity#getStatus()
 */
public enum JobStatus {
  /**
   * Job is queued and waiting for execution. Visible to polling queries when scheduled_time &lt;=
   * now. Transitions to RUNNING when picked up by a worker.
   */
  PENDING,

  /**
   * Job is actively executing on a worker node. Has exclusive ownership by the executing node.
   * Protected from duplicate execution via optimistic locking.
   */
  RUNNING,

  /**
   * Job completed successfully without errors. Terminal state. May trigger dependent jobs. Eligible
   * for archival after retention period.
   */
  SUCCEEDED,

  /**
   * Job execution failed with an error. May transition back to PENDING if retries remain. Terminal
   * state if max_retries exhausted. Backoff policy applied before retry.
   */
  FAILED,

  /**
   * Job was explicitly canceled and will not execute. Terminal state. Cancels any pending retries.
   */
  CANCELED,

  /**
   * Job is temporarily paused and will not execute until resumed. NOT visible to polling queries.
   * Preserves all job state including retry attempts. Transitions back to PENDING when resumed.
   */
  PAUSED;

  /**
   * Whether this status is a terminal state — a state from which the job will not transition back
   * to an executing status on its own. Used by retry and cascade code that must not silently
   * overwrite a terminal transition (e.g. a CANCEL racing with a cascade update).
   *
   * <p>Terminal states are {@link #SUCCEEDED}, {@link #FAILED}, and {@link #CANCELED}. Note that
   * {@code FAILED} is treated as terminal even though a failed job may later transition back to
   * {@code PENDING} via the retry path — the retry is driven by external logic that inspects {@code
   * attempts} and {@code maxRetries} before scheduling, not by the enum value itself. From the
   * enum's perspective, each snapshot of {@code FAILED} is terminal.
   */
  public boolean isTerminal() {
    return this == SUCCEEDED || this == FAILED || this == CANCELED;
  }
}
