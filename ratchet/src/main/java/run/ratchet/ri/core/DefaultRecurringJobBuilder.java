package run.ratchet.ri.core;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;

/** {@inheritDoc} */
class DefaultRecurringJobBuilder implements RecurringJobBuilder {

  private final String cronExpr;
  private final ZoneId zone;
  private final SerializableCheckedRunnable task;
  private final RecurringJobSubmitter submitter;

  private JobOptions options = JobOptions.defaults();
  private List<String> tags = List.of();
  private String businessKey;

  DefaultRecurringJobBuilder(
      String cronExpr,
      ZoneId zone,
      SerializableCheckedRunnable task,
      RecurringJobSubmitter submitter) {
    this.cronExpr = cronExpr;
    this.zone = zone;
    this.task = task;
    this.submitter = submitter;
  }

  @Override
  public RecurringJobBuilder withOptions(JobOptions options) {
    this.options = Objects.requireNonNull(options, "options must not be null");
    return this;
  }

  @Override
  public RecurringJobBuilder withTags(List<String> tags) {
    this.tags = tags == null ? List.of() : List.copyOf(tags);
    return this;
  }

  @Override
  public RecurringJobBuilder withBusinessKey(String key) {
    this.businessKey = (key != null && !key.isBlank()) ? key.trim() : null;
    return this;
  }

  @Override
  public JobHandle submit() {
    return submitter.submit(this);
  }

  String cronExpr() {
    return cronExpr;
  }

  ZoneId zone() {
    return zone;
  }

  SerializableCheckedRunnable task() {
    return task;
  }

  JobOptions options() {
    return options;
  }

  List<String> tags() {
    return tags;
  }

  String businessKey() {
    return businessKey;
  }
}
