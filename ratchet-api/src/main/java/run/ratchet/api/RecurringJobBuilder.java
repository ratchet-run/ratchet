package run.ratchet.api;

import java.util.List;

/**
 * Builder interface for configuring and submitting recurring jobs.
 *
 * <p>RecurringJobBuilder provides a fluent API for constructing jobs that execute on a recurring
 * schedule. It allows customization of job execution parameters, tagging for categorization or
 * grouping, and convenient submission to the scheduler.
 *
 * <h2>Methods:</h2>
 *
 * <ul>
 *   <li><b>withOptions(JobOptions options):</b> Configures advanced execution options for the
 *       recurring job, such as retry policies, timeouts, and priorities.
 *   <li><b>withTags(List<String> tags):</b> Associates tags with the job for easier identification,
 *       filtering, and categorization.
 *   <li><b>submit():</b> Finalizes the configuration and submits the job to the scheduler,
 *       returning a {@link JobHandle} as a reference.
 * </ul>
 *
 * <h2>Thread-Safety:</h2>
 *
 * <p>Implementations of this interface are typically not thread-safe. Instances of
 * RecurringJobBuilder should not be reused concurrently unless explicitly documented as safe for
 * such use.
 *
 * <h2>Behavior:</h2>
 *
 * <ul>
 *   <li>Configuration methods return the builder instance, allowing method chaining.
 *   <li>Once a job is submitted using {@code submit()}, the builder instance is no longer usable,
 *       and a new instance must be created for configuring another job.
 * </ul>
 *
 * <h2>Expected Usage:</h2>
 *
 * <p>RecurringJobBuilder is used by clients to create recurring scheduled jobs with consistent and
 * configurable behavior. It is generally obtained from a scheduler or factory method within the
 * scheduling system.
 *
 * @see JobOptions
 * @see JobHandle
 */
public interface RecurringJobBuilder {

  /**
   * Configures advanced execution options for the recurring job.
   *
   * <p>This method allows you to specify job execution behavior such as retry policies, timeouts,
   * and priorities using a {@link JobOptions} object. The provided options are immutable and will
   * define how the job is processed by the scheduler.
   *
   * @param options the {@link JobOptions} instance containing the desired execution settings. Must
   *     not be null.
   * @return the {@link RecurringJobBuilder} instance for chaining further configuration calls.
   *     Returns the same builder instance with the applied options.
   */
  RecurringJobBuilder withOptions(JobOptions options);

  /**
   * Associates a list of tags with the recurring job for easier identification, filtering, and
   * categorization within the scheduling system.
   *
   * <p>Tags provide a flexible mechanism for organizing and managing jobs by assigning semantic
   * labels. They can be used for grouping related jobs, applying filters in queries, or supporting
   * user-defined metadata.
   *
   * @param tags the list of tags to associate with the job. Each tag should be a non-null and
   *     non-empty string. Passing null or an empty list results in no tags being assigned to the
   *     job.
   * @return the {@code RecurringJobBuilder} instance for chaining further configuration calls.
   *     Returns the same builder instance with the applied tags.
   */
  RecurringJobBuilder withTags(List<String> tags);

  /**
   * Sets the business key for the recurring job.
   *
   * <p>The business key provides active-unique identity for recurring jobs. While the job is in an
   * active state (PENDING, RUNNING, PAUSED), no other job may share the same business key. Once the
   * job reaches a terminal state (SUCCEEDED, FAILED, CANCELED), the key becomes available for
   * reuse.
   *
   * <p>For {@link Recurring @Recurring} annotated methods, the annotation's {@link Recurring#id()
   * id} is automatically used as the business key, ensuring exactly one active recurring master
   * exists per annotation.
   *
   * @param key the business key. If null or blank, no business key is assigned.
   * @return the current {@code RecurringJobBuilder} instance for chaining.
   */
  RecurringJobBuilder withBusinessKey(String key);

  /**
   * Submits the configured recurring job to the scheduler for execution.
   *
   * <p>This method finalizes the current configuration and enqueues the job based on the provided
   * settings. Once submitted, the job will be executed according to the specified recurring
   * schedule and any additional execution options defined.
   *
   * <p>Jobs submitted through this method are assigned a unique identifier that can be used to
   * track, monitor, or manage their state. The returned {@code JobHandle} serves as a reference to
   * the submitted job, allowing interaction with its lifecycle after submission.
   *
   * @return a {@link JobHandle} representing the submitted job. The handle allows clients to
   *     retrieve the job's unique identifier and facilitates subsequent tracking or status queries.
   */
  JobHandle submit();
}
