package run.ratchet.api;

/**
 * Job priority levels for the scheduler. Higher ordinal values indicate higher priority.
 *
 * <p><b>WARNING: Ordinal values are persisted in the database.</b> Do NOT reorder, insert between,
 * or remove existing entries. Doing so will silently corrupt the priority of all existing jobs in
 * the {@code scheduler_job} table.
 *
 * <p>Current ordinal mapping:
 *
 * <ul>
 *   <li>{@link #LOWEST} = 0
 *   <li>{@link #LOW} = 1
 *   <li>{@link #NORMAL} = 2 (default)
 *   <li>{@link #HIGH} = 3
 *   <li>{@link #CRITICAL} = 4
 * </ul>
 *
 * <p>New priorities must only be appended to the end of this enum.
 */
public enum JobPriority {
  LOWEST,
  LOW,
  NORMAL,
  HIGH,
  CRITICAL
}
