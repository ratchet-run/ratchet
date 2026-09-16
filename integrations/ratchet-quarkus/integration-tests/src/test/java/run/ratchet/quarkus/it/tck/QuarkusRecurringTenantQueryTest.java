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
package run.ratchet.quarkus.it.tck;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.*;
import run.ratchet.spi.JobAuthorizationPolicy;

@QuarkusTest
@TestProfile(QuarkusRecurringTenantQueryTest.TenantProfile.class)
public class QuarkusRecurringTenantQueryTest {
  @Inject QuarkusRatchetTckRuntime runtime;
  @Inject JobSchedulerService scheduler;
  @Inject JobQueryService queries;

  @BeforeEach
  void before() {
    runtime.clear();
  }

  @AfterEach
  void after() {
    runtime.clear();
  }

  @Test
  void authorizationScopesRecurringCountsAndCursorPagesAtTheStore() {
    JobHandle a = master("tenant-a");
    master("tenant-b");
    JobHandle b = master("tenant-a");
    JobFilter filter =
        JobFilter.builder()
            .types(JobType.RECURRING)
            .tags("tenant-b")
            .statuses(JobStatus.PAUSED)
            .sortField(JobQuerySortField.CREATED_AT)
            .sortAscending(true)
            .build();
    JobPage<JobSummary> first = queries.findJobs(filter, 1, 0);
    assertEquals(2, first.totalCount());
    assertEquals(1, first.items().size());
    assertTrue(first.hasMore());
    assertNotNull(first.nextCursor());
    JobPage<JobSummary> second =
        queries.findJobs(filter.toBuilder().cursor(first.nextCursor()).build(), 1, 0);
    assertEquals(1, second.items().size());
    assertFalse(second.hasMore());
    assertEquals(
        Set.of(a.id(), b.id()), Set.of(first.items().get(0).id(), second.items().get(0).id()));
    JobPage<JobSummary> absent =
        queries.findJobs(filter.toBuilder().propertyEquals("tenant", "absent").build(), 10, 0);
    assertTrue(absent.items().isEmpty());
    assertEquals(0, absent.totalCount());
  }

  JobHandle master(String tenant) {
    JobHandle handle =
        scheduler
            .scheduleRecurringUtc("0 0 0 1 1 ?", CompletionProbeJobs::root)
            .withTags(List.of(tenant))
            .submit();
    assertTrue(scheduler.pauseJob(handle.id()));
    return handle;
  }

  public static class TenantProfile extends QuarkusRatchetTckProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(TenantPolicy.class);
    }
  }

  @Alternative
  @ApplicationScoped
  public static class TenantPolicy implements JobAuthorizationPolicy {
    @Override
    public JobFilter filterForPrincipal(JobFilter filter, String principal) {
      return filter.toBuilder().tags("tenant-a").build();
    }

    @Override
    public void checkCreate(UUID id, String principal) {}

    @Override
    public void checkCancel(UUID id, String owner, String principal) {}

    @Override
    public void checkPause(UUID id, String owner, String principal) {}

    @Override
    public void checkResume(UUID id, String owner, String principal) {}

    @Override
    public void checkRetry(UUID id, String owner, String principal) {}
  }
}
