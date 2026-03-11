package run.ratchet.ri.util;

/**
 * Centralized constants for the job scheduler framework to eliminate duplicated string literals
 * flagged by SonarQube S1192 (String literals should not be duplicated).
 *
 * <p>This class consolidates repeated string literals across scheduler components into well-named
 * constants, improving code maintainability and reducing the risk of typos in frequently used
 * values.
 *
 * <p>Constants are organized into categories:
 *
 * <ul>
 *   <li><b>Query Parameters:</b> JPA/SQL named parameter names
 *   <li><b>Metrics Tags:</b> Micrometer tag keys and values
 *   <li><b>Log Messages:</b> Frequently used log message fragments
 * </ul>
 */
public final class SchedulerConstants {

  // ============ JPA Query Parameter Names ============

  /** Component tag value for job-scheduler metrics. */
  public static final String COMPONENT_JOB_SCHEDULER = "job-scheduler";

  /** Component tag value for scheduler metrics. */
  public static final String COMPONENT_SCHEDULER = "scheduler";

  /** Log message fragment for jobs already in terminal state. */
  public static final String MSG_ALREADY_IN_TERMINAL_STATE = " already in terminal state ";

  /** Warning message when clustered counter increment fails and falls back to local counter. */
  public static final String MSG_CLUSTERED_COUNTER_FALLBACK =
      "Failed to increment clustered counter, using local fallback";

  // ============ Metrics Tag Keys ============

  /** Log message suffix for idempotent operations. */
  public static final String MSG_IDEMPOTENT = " (idempotent)";

  /** Log message suffix for bulk operation context. */
  public static final String MSG_IN_BULK_OPERATION = " in bulk operation";

  // ============ Metrics Tag Values ============

  /** Log message fragment for job context in error messages. */
  public static final String MSG_IN_JOB = " in job ";

  /** Log message prefix for skipped job operations. */
  public static final String MSG_SKIPPING_JOB = "Skipping job ";

  /** Named parameter for batch ID in queries. */
  public static final String PARAM_BATCH_ID = "batchId";

  // ============ Metrics Tag Values - Services ============

  /** Named parameter for parent job ID in workflow queries. */
  public static final String PARAM_PARENT_JOB_ID = "parentJobId";

  // ============ Log Message Fragments ============

  /** Named parameter for since/cutoff time in queries. */
  public static final String PARAM_SINCE = "since";

  /** Named parameter for status field in queries. */
  public static final String PARAM_STATUS = "status";

  /** Micrometer tag key for component identification. */
  public static final String TAG_COMPONENT = "component";

  /** Micrometer tag key for job type categorization. */
  public static final String TAG_JOB_TYPE = "job.type";

  /** Service tag value used in circuit breaker metrics. */
  public static final String TAG_VALUE_SERVICE = "service";

  /** Base unit for percentage metrics. */
  public static final String UNIT_PERCENT = "percent";

  private SchedulerConstants() {
    throw new UnsupportedOperationException("Utility class - do not instantiate");
  }
}
