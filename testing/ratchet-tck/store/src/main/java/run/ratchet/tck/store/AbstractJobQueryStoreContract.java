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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.query.JobQueryCursor;

/**
 * Base contract tests for {@link run.ratchet.store.spi.JobQueryStore} — dashboard-oriented search
 * and filter semantics.
 */
public abstract class AbstractJobQueryStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupQueryFixture() {
    cleanupStore();
  }

  // ── Status filtering ──────────────────────────────────────────────────

  @Test
  void searchByStatus_returnsOnlyMatchingJobs() {
    persist(newPendingJob());
    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().statuses(JobStatus.PENDING).build(), 100, 0);

    assertFalse(results.isEmpty(), "searchByStatus(PENDING) should return results");
    results.forEach(
        j ->
            assertEquals(JobStatus.PENDING, j.getStatus(), "All results must have status PENDING"));
  }

  @Test
  void searchByStatus_excludesNonMatchingJobs() {
    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().statuses(JobStatus.CANCELED).build(), 100, 0);

    assertTrue(
        results.isEmpty(),
        "searchByStatus(CANCELED) should return empty when no jobs are canceled");
  }

  @Test
  void searchByMultipleStatuses_returnsUnion() {
    var a = persist(newPendingJob());
    JobEntity running = newPendingJob();
    running.setStatus(JobStatus.RUNNING);
    var b = persist(running);

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder().statuses(JobStatus.PENDING, JobStatus.RUNNING).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(a.getId()), "Multi-status filter should include PENDING job");
    assertTrue(ids.contains(b.getId()), "Multi-status filter should include RUNNING job");
  }

  // ── Priority filtering ─────────────────────────────────────────────────

  @Test
  void searchByPriority_returnsMatchingJobs() {
    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    high = persist(high);

    JobEntity normal = newPendingJob();
    normal.setPriority(JobPriority.NORMAL);
    persist(normal);

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().priorities(JobPriority.HIGH).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(
        ids.contains(high.getId()), "Filter by HIGH priority should return the high-priority job");
    ids.forEach(
        id ->
            assertNotEquals(
                id,
                normal.getId(),
                "Filter by HIGH priority should not return NORMAL-priority job"));
  }

  // ── Job type filtering ─────────────────────────────────────────────────

  @Test
  void searchByJobType_returnsMatchingJobs() {
    persist(newPendingJob());
    JobEntity batch = newBatchParentJob();
    batch = persist(batch);
    UUID batchId = batch.getId();

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().types(JobType.BATCH).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(batchId), "Filter by BATCH type should return the batch parent job");
  }

  @Test
  void searchByJobType_excludesOtherTypes() {
    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().types(JobType.WORKFLOW).build(), 100, 0);

    assertTrue(
        results.isEmpty(),
        "Filter by WORKFLOW type should return empty when no workflow jobs exist");
  }

  // ── Business key filtering ─────────────────────────────────────────────

  @Test
  void searchByBusinessKey_returnsExactMatch() {
    JobEntity a = newPendingJob();
    a.setBusinessKey("order-42");
    a = persist(a);

    JobEntity b = newPendingJob();
    b.setBusinessKey("order-99");
    persist(b);

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().businessKey("order-42").build(), 100, 0);

    assertEquals(1, results.size(), "Business key filter should return exactly one match");
    assertEquals(
        a.getId(), results.get(0).getId(), "Returned job should match the given business key");
  }

  @Test
  void searchByBusinessKey_noMatch_returnsEmpty() {
    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().businessKey("nonexistent-key").build(), 100, 0);

    assertTrue(results.isEmpty(), "Business key filter with no match should return empty");
  }

  // ── Tag filtering ──────────────────────────────────────────────────────

  @Test
  void searchByTag_returnsJobsWithTag() {
    var tagged = persist(newPendingJob("billing"));
    persist(newPendingJob("shipping"));

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().tags("billing").build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(tagged.getId()), "Tag filter should return the tagged job");
    assertEquals(1, results.size(), "Tag filter should not return jobs without the tag");
  }

  @Test
  void searchByTag_noMatch_returnsEmpty() {
    persist(newPendingJob("billing"));

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().tags("nonexistent-tag").build(), 100, 0);

    assertTrue(results.isEmpty(), "Tag filter with no match should return empty");
  }

  // ── Tag id lookup (findJobIdsByTag) ────────────────────────────────────

  @Test
  void findJobIdsByTag_respectsPagination() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    var third = persist(newPendingJob());

    store().insertTags(first.getId(), List.of("shared"));
    store().insertTags(second.getId(), List.of("shared"));
    store().insertTags(third.getId(), List.of("shared"));

    List<UUID> page1 = queryStore().findJobIdsByTag("shared", 2, 0);
    List<UUID> page2 = queryStore().findJobIdsByTag("shared", 2, 2);

    assertEquals(2, page1.size(), "First page should contain 2 results");
    assertEquals(1, page2.size(), "Second page should contain 1 result");
    assertTrue(
        page1.stream().noneMatch(page2::contains),
        "Second page should contain ids not returned on the first page");
    assertEquals(
        List.of(first.getId(), second.getId(), third.getId()).stream()
            .filter(page2::contains)
            .count(),
        page2.size(),
        "Second page should contain one of the remaining tagged jobs");
  }

  @Test
  void findJobIdsByTag_returnsDeterministicIdOrder() {
    var third = newPendingJob();
    third.setId(new UUID(0L, 3L));
    store().create(third);

    var first = newPendingJob();
    first.setId(new UUID(0L, 1L));
    store().create(first);

    var second = newPendingJob();
    second.setId(new UUID(0L, 2L));
    store().create(second);

    store().insertTags(third.getId(), List.of("ordered-tag"));
    store().insertTags(first.getId(), List.of("ordered-tag"));
    store().insertTags(second.getId(), List.of("ordered-tag"));

    assertEquals(
        List.of(first.getId(), second.getId(), third.getId()),
        queryStore().findJobIdsByTag("ordered-tag", 10, 0),
        "tag scans should return deterministic ascending job IDs");
  }

  @Test
  void findJobIdsByTag_unknownTag_returnsEmpty() {
    List<UUID> ids = queryStore().findJobIdsByTag("nonexistent-tag", 10, 0);

    assertTrue(ids.isEmpty(), "findJobIdsByTag with unknown tag should return empty");
  }

  @Test
  void findJobIdsByTag_paginationOffset_skipsRows() {
    for (int i = 0; i < 5; i++) {
      var job = persist(newPendingJob());
      store().insertTags(job.getId(), List.of("offset-tag"));
    }

    List<UUID> page = queryStore().findJobIdsByTag("offset-tag", 10, 3);

    assertEquals(2, page.size(), "Offset 3 with 5 total should return 2 results");
  }

  // ── Caller principal filtering ─────────────────────────────────────────

  @Test
  void searchByCallerPrincipal_returnsMatchingJobs() {
    JobEntity owned = newPendingJob();
    owned.setCallerPrincipal("alice");
    owned = persist(owned);

    JobEntity other = newPendingJob();
    other.setCallerPrincipal("bob");
    persist(other);

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().callerPrincipal("alice").build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(owned.getId()), "CallerPrincipal filter should return alice's job");
    assertFalse(ids.contains(other.getId()), "CallerPrincipal filter should not return bob's job");
  }

  // ── Date range filtering ───────────────────────────────────────────────

  @Test
  void searchByCreatedAfter_returnsJobsInRange() {
    Instant cutoff = Instant.now().minus(1, ChronoUnit.HOURS);

    JobEntity recent = newPendingJob();
    recent = persist(recent);

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().createdAfter(cutoff).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(
        ids.contains(recent.getId()), "createdAfter filter should include recently created job");
  }

  @Test
  void searchByCreatedBefore_excludesFutureJobs() {
    Instant pastCutoff = Instant.now().minus(1, ChronoUnit.DAYS);
    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().createdBefore(pastCutoff).build(), 100, 0);

    assertTrue(
        results.isEmpty(),
        "createdBefore(yesterday) should return empty for jobs created just now");
  }

  // ── Pagination ─────────────────────────────────────────────────────────

  @Test
  void pagination_offsetZero_returnsFirstPage() {
    for (int i = 0; i < 5; i++) {
      persist(newPendingJob());
    }

    List<JobEntity> page = queryStore().searchJobs(JobFilter.builder().build(), 2, 0);

    assertEquals(2, page.size(), "Limit 2 offset 0 should return 2 results");
  }

  @Test
  void pagination_offsetN_skipsEarlierResults() {
    for (int i = 0; i < 5; i++) {
      persist(newPendingJob());
    }

    List<JobEntity> page1 = queryStore().searchJobs(JobFilter.builder().build(), 3, 0);
    List<JobEntity> page2 = queryStore().searchJobs(JobFilter.builder().build(), 3, 3);

    assertFalse(
        page1.stream().anyMatch(j -> page2.stream().anyMatch(k -> k.getId().equals(j.getId()))),
        "Page 1 and Page 2 should not overlap");
  }

  // ── Count ──────────────────────────────────────────────────────────────

  @Test
  void countJobs_matchesSearchResultSize() {
    for (int i = 0; i < 4; i++) {
      persist(newPendingJob());
    }
    JobFilter filter = JobFilter.builder().statuses(JobStatus.PENDING).build();

    long count = queryStore().countJobs(filter);
    List<JobEntity> all = queryStore().searchJobs(filter, 1000, 0);

    assertEquals(all.size(), count, "countJobs must equal the full result set size");
  }

  @Test
  void searchWithNoFilter_returnsAllJobs() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    List<JobEntity> results = queryStore().searchJobs(JobFilter.builder().build(), 1000, 0);

    assertEquals(3, results.size(), "Empty filter should return all persisted jobs");
  }

  @Test
  void searchWithNoMatchingFilter_returnsEmpty() {
    List<JobEntity> results =
        queryStore()
            .searchJobs(JobFilter.builder().businessKey("absolutely-nonexistent").build(), 100, 0);

    assertTrue(results.isEmpty(), "Filter matching nothing should return empty list");
  }

  @Test
  void countJobs_noMatchingFilter_returnsZero() {
    persist(newPendingJob());

    long count = queryStore().countJobs(JobFilter.builder().businessKey("no-such-key").build());

    assertEquals(0L, count, "countJobs with no matching filter should return 0");
  }

  // ── Sorting ────────────────────────────────────────────────────────────

  @Test
  void sortByCreatedAt_ascending_isOrdered() {
    for (int i = 0; i < 3; i++) {
      persist(newPendingJob());
    }
    JobFilter filter =
        JobFilter.builder().sortField(JobQuerySortField.CREATED_AT).sortAscending(true).build();

    List<JobEntity> results = queryStore().searchJobs(filter, 100, 0);

    assertEquals(3, results.size(), "Should return all 3 persisted results for ordering check");
    for (int i = 1; i < results.size(); i++) {
      Instant prev = results.get(i - 1).getCreatedAt();
      Instant curr = results.get(i).getCreatedAt();
      assertNotNull(prev, "Sorted jobs must expose non-null createdAt values");
      assertNotNull(curr, "Sorted jobs must expose non-null createdAt values");
      assertFalse(prev.isAfter(curr), "Results must be in ascending createdAt order");
    }
  }

  @Test
  void sortByPriority_descending_isOrdered() {
    JobEntity low = newPendingJob();
    low.setPriority(JobPriority.LOW);
    persist(low);

    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    persist(high);

    JobFilter filter =
        JobFilter.builder().sortField(JobQuerySortField.PRIORITY).sortAscending(false).build();

    List<JobEntity> results = queryStore().searchJobs(filter, 100, 0);

    assertEquals(2, results.size(), "Should return exactly 2 results for ordering check");
    for (int i = 1; i < results.size(); i++) {
      JobPriority prev = results.get(i - 1).getPriority();
      JobPriority curr = results.get(i).getPriority();
      assertNotNull(prev, "Sorted jobs must expose non-null priority values");
      assertNotNull(curr, "Sorted jobs must expose non-null priority values");
      assertTrue(prev.ordinal() >= curr.ordinal(), "Results must be in descending priority order");
    }
  }

  // ── traceCorrelationId filtering ───────────────────────────────────────

  @Test
  void searchByTraceCorrelationId_returnsMatchingJob() {
    String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    JobEntity traced = newPendingJob();
    traced.setTraceContext(Map.of("traceparent", traceparent));
    traced = persist(traced);

    persist(newPendingJob()); // no trace context

    List<JobEntity> results =
        queryStore()
            .searchJobs(JobFilter.builder().traceCorrelationId(traceparent).build(), 100, 0);

    assertFalse(results.isEmpty(), "traceCorrelationId filter should return the traced job");
    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(
        ids.contains(traced.getId()),
        "traceCorrelationId filter should include the job with matching traceparent");
  }

  @Test
  void searchByTraceCorrelationId_excludesNonMatchingJobs() {
    JobEntity traced = newPendingJob();
    traced.setTraceContext(Map.of("traceparent", "00-aabbcc-ddeeff-01"));
    persist(traced);

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder().traceCorrelationId("00-xxxxxx-yyyyyy-01").build(), 100, 0);

    assertTrue(
        results.isEmpty(),
        "traceCorrelationId filter should not match jobs with a different traceparent");
  }

  // ── Sort tiebreaker determinism ────────────────────────────────────────

  @Test
  void sortByCreatedAt_isStable_acrossConsecutivePages() {
    for (int i = 0; i < 6; i++) {
      persist(newPendingJob());
    }

    List<JobEntity> page1 = queryStore().searchJobs(JobFilter.builder().build(), 3, 0);
    List<JobEntity> page2 = queryStore().searchJobs(JobFilter.builder().build(), 3, 3);

    List<UUID> allIds = new ArrayList<>();
    page1.forEach(j -> allIds.add(j.getId()));
    page2.forEach(j -> allIds.add(j.getId()));

    List<JobEntity> allJobs = new ArrayList<>();
    allJobs.addAll(page1);
    allJobs.addAll(page2);
    for (int i = 1; i < allJobs.size(); i++) {
      assertDefaultCreatedAtOrder(allJobs.get(i - 1), allJobs.get(i));
    }

    assertEquals(
        allIds.size(),
        new HashSet<>(allIds).size(),
        "Consecutive pages must not contain duplicate job IDs");
  }

  // ── skipCount ─────────────────────────────────────────────────────────

  @Test
  void countJobs_returnsCorrectCount() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    long count = queryStore().countJobs(JobFilter.builder().statuses(JobStatus.PENDING).build());

    assertEquals(3L, count, "countJobs should return exactly 3 for 3 persisted PENDING jobs");
  }

  // ── Parent job filtering ───────────────────────────────────────────────

  @Test
  void searchByParentJobId_returnsOnlyDependants() {
    JobEntity parent = persist(newPendingJob());

    JobEntity child = newPendingJob();
    child.setDependsOn(parent.getId());
    child = persist(child);

    persist(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().parentJobId(parent.getId()).build(), 100, 0);

    assertEquals(1, results.size(), "parentJobId filter should return only direct dependants");
    assertEquals(child.getId(), results.get(0).getId(), "Returned job should be the child job");
  }

  // ── Archive-inclusive search (UNION over live + archive tables) ─────────

  @Test
  void searchIncludeArchived_returnsLiveAndArchivedRowsOnce() {
    JobEntity live = persist(newPendingJob());
    UUID archivedId = archiveOnly(newPendingJob());

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().includeArchived(true).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(live.getId()), "includeArchived search must still return live jobs");
    assertTrue(ids.contains(archivedId), "includeArchived search must return archived jobs");
    assertEquals(
        ids.size(),
        new HashSet<>(ids).size(),
        "An archived job must appear once, not duplicated across the live/archive UNION");
  }

  @Test
  void searchIncludeArchived_hydratesArchivedColumnsToCorrectFields() {
    JobEntity pending = newPendingJob();
    pending.setBusinessKey("bk-archived-search");
    UUID archivedId = archiveOnly(pending);

    List<JobEntity> results =
        queryStore().searchJobs(JobFilter.builder().includeArchived(true).build(), 100, 0);

    JobEntity archived =
        results.stream()
            .filter(j -> archivedId.equals(j.getId()))
            .findFirst()
            .orElseThrow(
                () -> new AssertionError("archived job missing from includeArchived search"));
    assertEquals(
        "bk-archived-search",
        archived.getBusinessKey(),
        "archive business_key must hydrate at the correct projection column");
    assertEquals(
        "com.example.TestJob",
        archived.getTargetClass(),
        "archive target_class must hydrate at the correct projection column");
    assertEquals(
        JobStatus.SUCCEEDED,
        archived.getStatus(),
        "archive final_status must hydrate as the job status");
    assertNotNull(archived.getCreatedAt(), "archive original_created_at must hydrate as createdAt");
  }

  @Test
  void searchIncludeArchived_sortsByPriorityDescendingAcrossBoundary() {
    JobEntity low = newPendingJob();
    low.setPriority(JobPriority.LOW);
    UUID liveLow = persist(low).getId();

    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    UUID archivedHigh = archiveOnly(high);

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder()
                    .includeArchived(true)
                    .sortField(JobQuerySortField.PRIORITY)
                    .sortAscending(false)
                    .build(),
                100,
                0);

    List<UUID> ids =
        results.stream().map(JobEntity::getId).filter(idsOf(liveLow, archivedHigh)).toList();
    assertEquals(
        List.of(archivedHigh, liveLow),
        ids,
        "PRIORITY-desc sort must place the archived HIGH job ahead of the live LOW job");
  }

  @Test
  void searchIncludeArchived_sortsByCreatedAtDescendingAcrossBoundary() {
    // Persist the live job first, then the archived job, so the archived job is the newest. With
    // both caller_principal values NULL, an ORDER BY that points at the wrong column collapses to
    // the job_id tiebreaker, which (UuidV7 being time-ordered) yields creation order — the reverse
    // of a correct created_at-descending sort.
    UUID liveOlder = persist(newPendingJob()).getId();
    spaceCreationTimestamps();
    UUID archivedNewer = archiveOnly(newPendingJob());

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder()
                    .includeArchived(true)
                    .sortField(JobQuerySortField.CREATED_AT)
                    .sortAscending(false)
                    .build(),
                100,
                0);

    List<JobEntity> mine =
        results.stream().filter(j -> idsOf(liveOlder, archivedNewer).test(j.getId())).toList();
    assertEquals(2, mine.size(), "Both the live and archived job must appear in the search");
    for (int i = 1; i < mine.size(); i++) {
      assertDefaultCreatedAtOrder(mine.get(i - 1), mine.get(i));
    }
    assertEquals(
        archivedNewer,
        mine.get(0).getId(),
        "CREATED_AT-desc sort must place the newer archived job first");
  }

  @Test
  void searchIncludeArchived_cursorPaginationOverArchiveVisitsEveryRowOnce() {
    // Give every archived row the same priority so the keyset tiebreaker — not the primary sort —
    // decides ordering. That is the slot where an archive cursor seeking the wrong id field drops
    // or repeats rows at the page boundary.
    int total = 7;
    Set<UUID> archivedIds = new HashSet<>();
    for (int i = 0; i < total; i++) {
      JobEntity job = newPendingJob();
      job.setPriority(JobPriority.NORMAL);
      archivedIds.add(archiveOnly(job));
    }

    int pageSize = 2;
    List<UUID> seen = new ArrayList<>();
    String cursor = null;
    for (int guard = 0; guard <= total; guard++) {
      var builder =
          JobFilter.builder()
              .includeArchived(true)
              .sortField(JobQuerySortField.PRIORITY)
              .sortAscending(false);
      if (cursor != null) {
        builder.cursor(cursor);
      }
      List<JobEntity> pageRows = queryStore().searchJobs(builder.build(), pageSize, 0);
      if (pageRows.isEmpty()) {
        break;
      }
      pageRows.forEach(r -> seen.add(r.getId()));
      JobEntity last = pageRows.get(pageRows.size() - 1);
      cursor =
          new JobQueryCursor(
                  JobQuerySortField.PRIORITY,
                  /* sortAscending= */ false,
                  Integer.toString(last.getPriority().ordinal()),
                  last.getId())
              .encode();
      if (pageRows.size() < pageSize) {
        break;
      }
    }

    Set<UUID> distinct = new HashSet<>(seen);
    assertEquals(
        seen.size(), distinct.size(), "Cursor pages over the archive must not repeat a row");
    assertEquals(
        archivedIds,
        distinct,
        "Cursor pages over the archive must visit every archived row exactly once");
  }

  @Test
  void searchJobs_liveCursorPaginationVisitsEveryRowOnce() {
    // Mirror the archive cursor-walk, but over the LIVE seek predicate (archive OFF). The live
    // appendCursorCondition tiebreaks on c.job_id — a column and table distinct from the archive
    // variant's a.original_job_id — and is only ever checked at the SQL-string level, never walked
    // end-to-end. Give every live row the same priority so the id tiebreaker, not the primary
    // sort, decides ordering: that is the slot where an off-by-one in
    // (col OP ? OR (col = ? AND c.job_id > ?)) drops or repeats a row at each page boundary.
    int total = 7;
    Set<UUID> seededIds = new HashSet<>();
    for (int i = 0; i < total; i++) {
      JobEntity job = newPendingJob();
      job.setPriority(JobPriority.NORMAL);
      seededIds.add(persist(job).getId());
    }

    int pageSize = 2;
    List<UUID> seen = new ArrayList<>();
    String cursor = null;
    for (int guard = 0; guard <= total; guard++) {
      var builder = JobFilter.builder().sortField(JobQuerySortField.PRIORITY).sortAscending(false);
      if (cursor != null) {
        builder.cursor(cursor);
      }
      List<JobEntity> pageRows = queryStore().searchJobs(builder.build(), pageSize, 0);
      if (pageRows.isEmpty()) {
        break;
      }
      pageRows.forEach(r -> seen.add(r.getId()));
      JobEntity last = pageRows.get(pageRows.size() - 1);
      cursor =
          new JobQueryCursor(
                  JobQuerySortField.PRIORITY,
                  /* sortAscending= */ false,
                  Integer.toString(last.getPriority().ordinal()),
                  last.getId())
              .encode();
      if (pageRows.size() < pageSize) {
        break;
      }
    }

    Set<UUID> distinct = new HashSet<>(seen);
    assertEquals(
        seen.size(), distinct.size(), "Live cursor pages must not repeat a row across boundaries");
    assertEquals(
        seededIds,
        distinct,
        "Live cursor pages must visit every matching row exactly once across page boundaries");
  }

  @Test
  void searchJobs_cursorMintedForADifferentSortIsIgnored() {
    // A keyset cursor records the sort it was produced under. The seek predicate filters on the
    // cursor's sort field while the ORDER BY comes from the live filter, so reusing a cursor after
    // changing the sort would seek on one axis while the query orders by another — silently
    // dropping or repeating rows. Page once under CREATED_AT, then reuse that cursor on a query
    // sorted by PRIORITY: the store must ignore the mismatched cursor and fall back to offset
    // paging, so the PRIORITY query returns the same rows with or without the stale cursor.
    List<JobEntity> persisted = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      persisted.add(persist(newPendingJob()));
      spaceCreationTimestamps();
    }
    Set<UUID> mine =
        persisted.stream().map(JobEntity::getId).collect(java.util.stream.Collectors.toSet());
    JobEntity anchor = persisted.get(2);

    String staleCursor =
        new JobQueryCursor(
                JobQuerySortField.CREATED_AT,
                /* sortAscending= */ true,
                anchor.getCreatedAt().toString(),
                anchor.getId())
            .encode();

    JobFilter priorityNoCursor =
        JobFilter.builder().sortField(JobQuerySortField.PRIORITY).sortAscending(false).build();
    JobFilter priorityStaleCursor =
        JobFilter.builder()
            .sortField(JobQuerySortField.PRIORITY)
            .sortAscending(false)
            .cursor(staleCursor)
            .build();

    List<UUID> baseline =
        queryStore().searchJobs(priorityNoCursor, 100, 0).stream()
            .map(JobEntity::getId)
            .filter(mine::contains)
            .toList();
    List<UUID> withStaleCursor =
        queryStore().searchJobs(priorityStaleCursor, 100, 0).stream()
            .map(JobEntity::getId)
            .filter(mine::contains)
            .toList();

    assertEquals(
        baseline,
        withStaleCursor,
        "A cursor minted for a different sort must be ignored, not applied as a seek");
  }

  /**
   * Creates a terminal job, archives it, and deletes the live cold row so the job exists only in
   * the archive table — the state an archive-inclusive search must surface from the archive branch.
   */
  private UUID archiveOnly(JobEntity pending) {
    JobEntity saved = persist(pending);
    UUID id = saved.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(id, null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    JobEntity completed = store().findById(id).orElseThrow();
    archiveStore().archiveJob(completed, "tck-archive-search", "tck");
    store().deleteJobsByIds(List.of(id));
    return id;
  }

  private static java.util.function.Predicate<UUID> idsOf(UUID... ids) {
    Set<UUID> set = new HashSet<>(Arrays.asList(ids));
    return set::contains;
  }

  private static void spaceCreationTimestamps() {
    // The store stamps created_at server-side at insert (a caller-set createdAt is ignored), so two
    // jobs persisted back-to-back can land in the same millisecond. A short pause guarantees a
    // distinct, ordered created_at for the cross-boundary sort assertion.
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while spacing creation timestamps", e);
    }
  }

  // ── Extension-property filtering ──────────────────────────────────────

  @Test
  void searchByPropertyEquals_returnsOnlyJobsCarryingTheProperty() {
    var tagged = persist(newPendingJob());
    var other = persist(newPendingJob());
    extensionStore().putProperty(tagged.getId(), "ratchet-tck.block_name", "invoice.send");
    extensionStore().putProperty(other.getId(), "ratchet-tck.block_name", "invoice.archive");

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder()
                    .propertyEquals("ratchet-tck.block_name", "invoice.send")
                    .build(),
                100,
                0);

    assertEquals(1, results.size(), "propertyEquals must match exactly the tagged job");
    assertEquals(tagged.getId(), results.get(0).getId());
  }

  @Test
  void searchByPropertyIn_matchesAnyListedValue() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    var third = persist(newPendingJob());
    extensionStore().putProperty(first.getId(), "ratchet-tck.block_name", "invoice.send");
    extensionStore().putProperty(second.getId(), "ratchet-tck.block_name", "invoice.archive");
    extensionStore().putProperty(third.getId(), "ratchet-tck.block_name", "invoice.void");

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder()
                    .propertyIn(
                        "ratchet-tck.block_name", List.of("invoice.send", "invoice.archive"))
                    .build(),
                100,
                0);

    assertEquals(2, results.size(), "propertyIn must match jobs carrying any listed value");
  }

  @Test
  void searchByMultiplePropertyKeys_combinesWithAnd() {
    var both = persist(newPendingJob());
    var nameOnly = persist(newPendingJob());
    extensionStore().putProperty(both.getId(), "ratchet-tck.block_name", "invoice.send");
    extensionStore().putProperty(both.getId(), "ratchet-tck.block_version", "2");
    extensionStore().putProperty(nameOnly.getId(), "ratchet-tck.block_name", "invoice.send");

    List<JobEntity> results =
        queryStore()
            .searchJobs(
                JobFilter.builder()
                    .propertyEquals("ratchet-tck.block_name", "invoice.send")
                    .propertyEquals("ratchet-tck.block_version", "2")
                    .build(),
                100,
                0);

    assertEquals(1, results.size(), "multiple property keys must intersect");
    assertEquals(both.getId(), results.get(0).getId());
  }

  @Test
  void countJobs_appliesPropertyFilters() {
    var tagged = persist(newPendingJob());
    persist(newPendingJob());
    extensionStore().putProperty(tagged.getId(), "ratchet-tck.block_name", "invoice.send");

    long count =
        queryStore()
            .countJobs(
                JobFilter.builder()
                    .propertyEquals("ratchet-tck.block_name", "invoice.send")
                    .build());

    assertEquals(1, count, "countJobs must apply property filters");
  }

  private static void assertDefaultCreatedAtOrder(JobEntity previous, JobEntity current) {
    assertNotNull(previous.getCreatedAt(), "Sorted jobs must expose non-null createdAt values");
    assertNotNull(current.getCreatedAt(), "Sorted jobs must expose non-null createdAt values");
    int createdOrder = previous.getCreatedAt().compareTo(current.getCreatedAt());
    if (createdOrder != 0) {
      assertTrue(createdOrder >= 0, "Default ordering must sort createdAt descending");
      return;
    }
    assertTrue(
        previous.getId().compareTo(current.getId()) <= 0,
        "Default ordering must use job id as a stable ascending tiebreaker");
  }
}
