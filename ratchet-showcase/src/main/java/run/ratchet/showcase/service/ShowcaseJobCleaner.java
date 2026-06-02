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
package run.ratchet.showcase.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobSummary;
import run.ratchet.showcase.jobs.ShowcaseJobs;
import run.ratchet.store.spi.JobStore;

@ApplicationScoped
public class ShowcaseJobCleaner {

  private static final int PAGE_SIZE = 500;
  private static final int MAX_PAGES = 20;
  private static final String SHOWCASE_JOBS_TARGET = ShowcaseJobs.class.getName();

  @Inject JobSchedulerService scheduler;
  @Inject JobQueryService jobQuery;
  @Inject JobStore jobStore;
  @Inject OrderRepository repository;
  @Inject ShowcaseCircuitBreakers circuitBreakers;

  public CleanupResult resetForFreshRun() {
    int recurringByTag = scheduler.cancelRecurringJobsByTag(ShowcaseTags.SHOWCASE);
    int recurringByBusinessKey =
        scheduler.cancelRecurringJobByBusinessKey(OrderStreamService.STREAM_BUSINESS_KEY);
    int activeByTag = scheduler.cancelJobsByTag(ShowcaseTags.SHOWCASE);
    int deletedByTarget = deleteJobs(JobFilter.builder().targetClass(SHOWCASE_JOBS_TARGET).build());
    int deletedByTag = deleteJobs(JobFilter.builder().tags(ShowcaseTags.SHOWCASE).build());
    repository.reset();
    circuitBreakers.resetAll();
    return new CleanupResult(
        recurringByTag, recurringByBusinessKey, activeByTag, deletedByTarget, deletedByTag);
  }

  private int deleteJobs(JobFilter filter) {
    int deleted = 0;
    for (int page = 0; page < MAX_PAGES; page++) {
      List<UUID> ids = page(filter);
      if (ids.isEmpty()) {
        return deleted;
      }
      int count = jobStore.deleteJobsByIds(ids);
      deleted += count;
      if (count == 0) {
        return deleted;
      }
    }
    return deleted;
  }

  private List<UUID> page(JobFilter filter) {
    Set<UUID> ids = new LinkedHashSet<>();
    for (JobSummary summary :
        jobQuery.findJobs(filter.toBuilder().skipCount(true).build(), PAGE_SIZE, 0).items()) {
      ids.add(summary.id());
    }
    return new ArrayList<>(ids);
  }

  public record CleanupResult(
      int recurringByTag,
      int recurringByBusinessKey,
      int activeByTag,
      int deletedByTarget,
      int deletedByTag) {}
}
