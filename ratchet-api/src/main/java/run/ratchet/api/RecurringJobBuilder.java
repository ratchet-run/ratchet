package run.ratchet.api;

import java.util.List;

/**
 * Fluent API for creating recurring jobs with cron expressions.
 *
 * <p>Concrete implementations are provided by the RI and obtained via {@link
 * JobSchedulerService#recurring(String, java.time.ZoneId, SerializableCheckedRunnable)}.
 */
public interface RecurringJobBuilder {

  RecurringJobBuilder withOptions(JobOptions options);

  RecurringJobBuilder withTags(List<String> tags);

  JobHandle submit();
}
