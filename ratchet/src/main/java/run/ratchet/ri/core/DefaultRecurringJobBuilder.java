/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.RecurringMisfirePolicy;
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
  private RecurringMisfirePolicy misfirePolicy = RecurringMisfirePolicy.defaults();
  private String executionTarget;
  private boolean encryptedPayload;

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
  public RecurringJobBuilder withMisfirePolicy(RecurringMisfirePolicy policy) {
    this.misfirePolicy = Objects.requireNonNull(policy, "policy must not be null");
    return this;
  }

  @Override
  public RecurringJobBuilder virtual() {
    this.executionTarget = ExecutorTargets.VIRTUAL;
    return this;
  }

  @Override
  public RecurringJobBuilder platform() {
    this.executionTarget = ExecutorTargets.PLATFORM;
    return this;
  }

  @Override
  public RecurringJobBuilder withEncryptedPayload() {
    this.encryptedPayload = true;
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

  RecurringMisfirePolicy misfirePolicy() {
    return misfirePolicy;
  }

  String executionTarget() {
    return executionTarget;
  }

  boolean encryptedPayload() {
    return encryptedPayload;
  }
}
