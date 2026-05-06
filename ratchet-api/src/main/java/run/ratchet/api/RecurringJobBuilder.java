package run.ratchet.api;

import java.util.List;

/**
 * Fluent builder for configuring and submitting recurring jobs.
 *
 * @see JobOptions
 * @see JobHandle
 */
public interface RecurringJobBuilder {

  RecurringJobBuilder withOptions(JobOptions options);

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
