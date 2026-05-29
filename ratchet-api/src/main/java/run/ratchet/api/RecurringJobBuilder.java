package run.ratchet.api;

import java.util.List;

/**
 * Fluent builder for configuring and submitting recurring jobs.
 *
 * @apiNote Unlike {@link JobBuilder}, this builder is intentionally submit-only and does not expose
 *     read-back accessors for the configured options, tags, or business key. Tooling that needs to
 *     inspect a recurring schedule after submission should query {@link JobQueryService} using the
 *     returned {@link JobHandle}.
 * @see JobOptions
 * @see JobHandle
 */
public interface RecurringJobBuilder {

  /**
   * Replaces the recurring job options used for children created from this schedule.
   *
   * <p>The default is {@link JobOptions#defaults()}. This is an in-memory builder operation; it
   * does not open a transaction.
   *
   * @param options non-null options to apply
   * @throws NullPointerException if {@code options} is null
   */
  RecurringJobBuilder withOptions(JobOptions options);

  /**
   * Replaces the recurring job tags.
   *
   * <p>The builder defensively copies the supplied list. Passing null clears the tags. Duplicate
   * tags are not significant; backing stores may collapse duplicates when persisting tags.
   *
   * @param tags replacement tags, or null for no tags
   * @throws NullPointerException if {@code tags} contains null elements
   */
  RecurringJobBuilder withTags(List<String> tags);

  /**
   * Sets the business key for active-unique identity. While the job is active (PENDING, RUNNING,
   * PAUSED), no other job may share the same key. For {@link Recurring @Recurring} methods, the
   * annotation's {@link Recurring#id() id} is used automatically.
   *
   * @param key the business key, or null/blank for none
   */
  RecurringJobBuilder withBusinessKey(String key);

  /**
   * Persists the recurring job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   */
  JobHandle submit();
}
