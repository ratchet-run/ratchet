package run.ratchet.api;

import java.util.List;

/**
 * Fluent builder for configuring and submitting recurring jobs.
 *
 * @see JobOptions
 * @see JobHandle
 */
public interface RecurringJobBuilder {

  /** Configures execution options (retry policy, timeout, priority, etc.) for the recurring job. */
  RecurringJobBuilder withOptions(JobOptions options);

  /** Associates tags with the recurring job for filtering and categorization. */
  RecurringJobBuilder withTags(List<String> tags);

  /**
   * Sets the business key for active-unique identity. While the job is active (PENDING, RUNNING,
   * PAUSED), no other job may share the same key. For {@link Recurring @Recurring} methods, the
   * annotation's {@link Recurring#id() id} is used automatically.
   *
   * @param key the business key, or null/blank for none
   */
  RecurringJobBuilder withBusinessKey(String key);

  /** Submits the configured recurring job to the scheduler. */
  JobHandle submit();
}
