package run.ratchet.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method to be executed as a recurring job based on a cron schedule.
 *
 * <p>The {@code @Recurring} annotation provides a declarative way to schedule methods for periodic
 * execution without explicit scheduler configuration. Annotated methods are automatically
 * discovered at application startup and registered with the job scheduler.
 *
 * <h2>Method Requirements:</h2>
 *
 * <p>Methods annotated with {@code @Recurring} must meet these requirements:
 *
 * <ul>
 *   <li>Must be public methods
 *   <li>Must have either no parameters or a single {@link JobContext} parameter
 *   <li>Must be part of a CDI-managed bean (e.g., {@code @ApplicationScoped}, {@code @Stateless})
 *   <li>Return type can be anything (return values are ignored)
 * </ul>
 *
 * <h2>Cron Expression Format:</h2>
 *
 * <p>Uses Quartz cron format with 6-7 fields:
 *
 * <pre>
 * second minute hour day-of-month month day-of-week [year]
 * </pre>
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * &#64;ApplicationScoped
 * public class MaintenanceService {
 *
 *     // Simple recurring job - runs at 2 AM daily
 *     &#64;Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
 *     public void performCleanup() {
 *         // Cleanup logic
 *     }
 *
 *     // Recurring job with context - runs every 30 minutes in New York timezone
 *     &#64;Recurring(cron = "0 *\/30 * * * ?", zone = "America/New_York")
 *     public void syncData(JobContext context) {
 *         String jobId = context.getJobId();
 *         // Sync logic with job context
 *     }
 *
 *     // High-priority job with custom retry policy
 *     &#64;Recurring(
 *         cron = "0 0 * * * ?",
 *         name = "Hourly Health Check",
 *         priority = 8,
 *         maxRetries = 5,
 *         backoffPolicy = BackoffPolicy.EXPONENTIAL,
 *         tags = {"health", "monitoring"}
 *     )
 *     public void healthCheck() {
 *         // Health check logic
 *     }
 * }
 * }</pre>
 *
 * <h2>Lifecycle:</h2>
 *
 * <p>Recurring jobs are scanned and registered during application startup. The scheduler maintains
 * a single definition per unique job ID and spawns individual job instances at each scheduled
 * execution time.
 *
 * @see JobContext
 * @see RecurringJobBuilder
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Recurring {

  /**
   * Initial backoff delay in milliseconds for retry attempts. The actual delay depends on the
   * backoff policy.
   */
  long backoffDelayMs() default 1000;

  /** The backoff policy to use between retry attempts. */
  BackoffPolicy backoffPolicy() default BackoffPolicy.EXPONENTIAL;

  /**
   * The cron expression defining when this job should run. Uses Quartz cron format: second minute
   * hour day-of-month month day-of-week [year]
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"0 0 2 * * ?" - Every day at 2 AM
   *   <li>"0 *\/15 * * * ?" - Every 15 minutes
   *   <li>"0 0 9 ? * MON" - Every Monday at 9 AM
   * </ul>
   */
  String cron();

  /**
   * Whether this recurring job is enabled. Can be used with property placeholders to conditionally
   * enable/disable jobs.
   *
   * <p>Example: enabled = "${app.maintenance.enabled:true}"
   */
  String enabled() default "true";

  /**
   * Unique identifier for this recurring job. If not specified, defaults to the fully qualified
   * class name + method name.
   *
   * <p>This ID is used as the business key to ensure idempotency and to manage the job lifecycle
   * (update, pause, resume, delete).
   */
  String id() default "";

  /** Maximum number of retry attempts if the job fails. */
  int maxRetries() default 3;

  /**
   * Human-readable name for this job. Used in monitoring and logs. If not specified, defaults to
   * the method name.
   */
  String name() default "";

  /**
   * Job execution priority. Higher priority jobs are executed before lower priority ones.
   *
   * <p>Maps to {@link JobPriority} enum ordinal values:
   *
   * <ul>
   *   <li>{@code 0} = LOWEST
   *   <li>{@code 1} = LOW
   *   <li>{@code 2} = NORMAL (default)
   *   <li>{@code 3} = HIGH
   *   <li>{@code 4} = CRITICAL
   * </ul>
   *
   * <p>The default value of 5 exceeds the enum range and is treated as NORMAL by the scheduler when
   * no explicit mapping is found.
   */
  int priority() default 5;

  /** Tags to associate with this job for filtering and categorization. */
  String[] tags() default {};

  /**
   * Maximum time in seconds this job is allowed to run before timing out. Default is 1 hour (3600
   * seconds).
   */
  long timeoutSeconds() default 3600;

  /**
   * The timezone for the cron expression. Defaults to UTC. Must be a valid {@link java.time.ZoneId}
   * string.
   */
  String zone() default "UTC";
}
