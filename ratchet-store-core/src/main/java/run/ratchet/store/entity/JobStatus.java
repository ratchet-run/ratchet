package run.ratchet.store.entity;

/**
 * Lifecycle states for a scheduled job.
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
  /** Visible to polling queries when scheduled_time &lt;= now. */
  PENDING,

  /** Exclusively owned by the executing node; protected via optimistic locking. */
  RUNNING,

  /** Terminal. Eligible for archival after retention period. */
  SUCCEEDED,

  /**
   * Terminal per enum snapshot. Transitions back to PENDING via the retry path when attempts &lt;
   * maxRetries — driven by external logic, not the enum value itself.
   */
  FAILED,

  /** Terminal. No further execution. */
  CANCELED,

  /** NOT visible to polling queries. Transitions back to PENDING when resumed. */
  PAUSED;

  /**
   * Returns true for {@link #SUCCEEDED}, {@link #FAILED}, and {@link #CANCELED}. Used by retry and
   * cascade code to avoid silently overwriting a terminal transition.
   */
  public boolean isTerminal() {
    return this == SUCCEEDED || this == FAILED || this == CANCELED;
  }
}
