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
        store().searchJobs(JobFilter.builder().statuses(JobStatus.PENDING).build(), 100, 0);

    assertFalse(results.isEmpty(), "searchByStatus(PENDING) should return results");
    results.forEach(
        j ->
            assertEquals(JobStatus.PENDING, j.getStatus(), "All results must have status PENDING"));
  }

  @Test
  void searchByStatus_excludesNonMatchingJobs() {
    persist(newPendingJob());

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().statuses(JobStatus.CANCELED).build(), 100, 0);

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
        store()
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
        store().searchJobs(JobFilter.builder().priorities(JobPriority.HIGH).build(), 100, 0);

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
        store().searchJobs(JobFilter.builder().types(JobType.BATCH).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(batchId), "Filter by BATCH type should return the batch parent job");
  }

  @Test
  void searchByJobType_excludesOtherTypes() {
    persist(newPendingJob());

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().types(JobType.WORKFLOW).build(), 100, 0);

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
        store().searchJobs(JobFilter.builder().businessKey("order-42").build(), 100, 0);

    assertEquals(1, results.size(), "Business key filter should return exactly one match");
    assertEquals(
        a.getId(), results.get(0).getId(), "Returned job should match the given business key");
  }

  @Test
  void searchByBusinessKey_noMatch_returnsEmpty() {
    persist(newPendingJob());

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().businessKey("nonexistent-key").build(), 100, 0);

    assertTrue(results.isEmpty(), "Business key filter with no match should return empty");
  }

  // ── Tag filtering ──────────────────────────────────────────────────────

  @Test
  void searchByTag_returnsJobsWithTag() {
    var tagged = persist(newPendingJob("billing"));
    persist(newPendingJob("shipping"));

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().tags("billing").build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(ids.contains(tagged.getId()), "Tag filter should return the tagged job");
    assertEquals(1, results.size(), "Tag filter should not return jobs without the tag");
  }

  @Test
  void searchByTag_noMatch_returnsEmpty() {
    persist(newPendingJob("billing"));

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().tags("nonexistent-tag").build(), 100, 0);

    assertTrue(results.isEmpty(), "Tag filter with no match should return empty");
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
        store().searchJobs(JobFilter.builder().callerPrincipal("alice").build(), 100, 0);

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
        store().searchJobs(JobFilter.builder().createdAfter(cutoff).build(), 100, 0);

    List<UUID> ids = results.stream().map(JobEntity::getId).toList();
    assertTrue(
        ids.contains(recent.getId()), "createdAfter filter should include recently created job");
  }

  @Test
  void searchByCreatedBefore_excludesFutureJobs() {
    Instant pastCutoff = Instant.now().minus(1, ChronoUnit.DAYS);
    persist(newPendingJob());

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().createdBefore(pastCutoff).build(), 100, 0);

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

    List<JobEntity> page = store().searchJobs(JobFilter.builder().build(), 2, 0);

    assertEquals(2, page.size(), "Limit 2 offset 0 should return 2 results");
  }

  @Test
  void pagination_offsetN_skipsEarlierResults() {
    for (int i = 0; i < 5; i++) {
      persist(newPendingJob());
    }

    List<JobEntity> page1 = store().searchJobs(JobFilter.builder().build(), 3, 0);
    List<JobEntity> page2 = store().searchJobs(JobFilter.builder().build(), 3, 3);

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

    long count = store().countJobs(filter);
    List<JobEntity> all = store().searchJobs(filter, 1000, 0);

    assertEquals(all.size(), count, "countJobs must equal the full result set size");
  }

  @Test
  void searchWithNoFilter_returnsAllJobs() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    List<JobEntity> results = store().searchJobs(JobFilter.builder().build(), 1000, 0);

    assertTrue(results.size() >= 3, "Empty filter should return all persisted jobs");
  }

  @Test
  void searchWithNoMatchingFilter_returnsEmpty() {
    List<JobEntity> results =
        store()
            .searchJobs(JobFilter.builder().businessKey("absolutely-nonexistent").build(), 100, 0);

    assertTrue(results.isEmpty(), "Filter matching nothing should return empty list");
  }

  @Test
  void countJobs_noMatchingFilter_returnsZero() {
    persist(newPendingJob());

    long count = store().countJobs(JobFilter.builder().businessKey("no-such-key").build());

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

    List<JobEntity> results = store().searchJobs(filter, 100, 0);

    assertTrue(results.size() >= 2, "Should return multiple results for ordering check");
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

    List<JobEntity> results = store().searchJobs(filter, 100, 0);

    assertTrue(results.size() >= 2, "Should return at least 2 results for ordering check");
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
        store().searchJobs(JobFilter.builder().traceCorrelationId(traceparent).build(), 100, 0);

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
        store()
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

    List<JobEntity> page1 = store().searchJobs(JobFilter.builder().build(), 3, 0);
    List<JobEntity> page2 = store().searchJobs(JobFilter.builder().build(), 3, 3);

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

    long count = store().countJobs(JobFilter.builder().statuses(JobStatus.PENDING).build());

    assertTrue(count >= 3, "countJobs should return at least 3 for 3 persisted PENDING jobs");
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
        store().searchJobs(JobFilter.builder().parentJobId(parent.getId()).build(), 100, 0);

    assertEquals(1, results.size(), "parentJobId filter should return only direct dependants");
    assertEquals(child.getId(), results.get(0).getId(), "Returned job should be the child job");
  }

  // ── Archive-inclusive search (UNION over live + archive tables) ─────────

  @Test
  void searchIncludeArchived_returnsLiveAndArchivedRowsOnce() {
    JobEntity live = persist(newPendingJob());
    UUID archivedId = archiveOnly(newPendingJob());

    List<JobEntity> results =
        store().searchJobs(JobFilter.builder().includeArchived(true).build(), 100, 0);

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
        store().searchJobs(JobFilter.builder().includeArchived(true).build(), 100, 0);

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
        store()
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
        store()
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
      List<JobEntity> pageRows = store().searchJobs(builder.build(), pageSize, 0);
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
        store().searchJobs(priorityNoCursor, 100, 0).stream()
            .map(JobEntity::getId)
            .filter(mine::contains)
            .toList();
    List<UUID> withStaleCursor =
        store().searchJobs(priorityStaleCursor, 100, 0).stream()
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
    store().archiveJob(completed, "tck-archive-search", "tck");
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
