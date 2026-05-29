package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.ExecutionHistorySummary;
import run.ratchet.api.JobDetail;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;
import run.ratchet.api.JobType;
import run.ratchet.api.QueueHealthSnapshot;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.query.JobQueryCursor;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobQueryStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobQueryServiceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobQueryStore queryStore;
  @Mock private JobCrudStore crudStore;
  @Mock private ExecutionStore executionStore;
  @Mock private run.ratchet.store.spi.RecurringJobStore recurringJobStore;
  @Mock private JobAuthorizationPolicy authPolicy;
  @Mock private CallerPrincipalProvider principalProvider;

  private DefaultJobQueryService service;

  private static boolean hasType(JobFilter filter, JobType type) {
    return filter != null && filter.types() != null && filter.types().contains(type);
  }

  // ── findJobs ────────────────────────────────────────────────────────────

  private static JobEntity minimalJob() {
    return minimalJobWithId(UUID.randomUUID());
  }

  private static JobEntity minimalJobWithId(UUID id) {
    JobEntity e = new JobEntity();
    e.setId(id);
    e.setStatus(JobStatus.PENDING);
    e.setJobType(JobExecutionType.SINGLE);
    e.setPriority(JobPriority.NORMAL);
    e.setBackoffPolicy(BackoffPolicy.NONE);
    e.setPayload(new JobPayload("com.example.TestJob", "run", "()V", false, List.of()));
    e.setCreatedAt(Instant.now());
    e.setScheduledTime(Instant.now());
    return e;
  }

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobQueryService(
            queryStore,
            crudStore,
            executionStore,
            recurringJobStore,
            authPolicy,
            principalProvider,
            FIXED_CLOCK);
    lenient().when(principalProvider.currentPrincipal()).thenReturn(Optional.empty());
    lenient()
        .when(authPolicy.filterForPrincipal(any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));
  }

  @Test
  void getRecurringMasters_readsFromRecurringJobStoreNotTheExecutableQueue() {
    UUID id = UUID.randomUUID();
    run.ratchet.store.spi.RecurringJobDefinition def =
        new run.ratchet.store.spi.RecurringJobDefinition(
            id,
            "0 * * * * ?",
            "UTC",
            Instant.parse("2026-05-20T12:00:00Z"),
            false,
            null,
            run.ratchet.api.JobPriority.NORMAL.ordinal(),
            3,
            run.ratchet.api.BackoffPolicy.NONE,
            0,
            0,
            new run.ratchet.store.entity.JobPayload(
                "com.example.Recurring", "tick", "()V", true, List.of()),
            null,
            null,
            "bk-rec",
            null,
            null,
            Instant.parse("2026-05-19T00:00:00Z"),
            "alice");
    when(recurringJobStore.listAll()).thenReturn(new java.util.ArrayList<>(List.of(def)));

    JobPage<JobSummary> page = service.getRecurringMasters(10, 0);

    assertEquals(1, page.items().size());
    JobSummary summary = page.items().get(0);
    assertEquals(id, summary.id());
    assertEquals(run.ratchet.api.JobType.RECURRING, summary.type());
    assertEquals("bk-rec", summary.businessKey());
    assertEquals("com.example.Recurring", summary.targetClass());
    verify(queryStore, never()).searchJobs(any(), anyInt(), anyInt());
  }

  @Test
  void findJobs_delegatesToQueryStore_andMapsResults() {
    JobEntity entity = minimalJob();
    when(queryStore.searchJobs(any(), eq(10), eq(0))).thenReturn(List.of(entity));
    when(queryStore.countJobs(any())).thenReturn(1L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 10, 0);

    assertEquals(1, page.items().size());
    assertEquals(1L, page.totalCount());
    assertEquals(entity.getId(), page.items().get(0).id());
  }

  @Test
  void findJobs_emptyResult_returnsEmptyPage() {
    when(queryStore.searchJobs(any(), anyInt(), anyInt())).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(any())).thenReturn(0L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 10, 0);

    assertTrue(page.items().isEmpty());
    assertEquals(0L, page.totalCount());
    assertFalse(page.hasMore());
    assertNull(page.nextCursor());
  }

  @Test
  void findJobs_nullFilterUsesEmptyFilter() {
    when(queryStore.searchJobs(any(), eq(10), eq(0))).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(any())).thenReturn(0L);

    JobPage<JobSummary> page = service.findJobs(null, 10, 0);

    assertTrue(page.items().isEmpty());
    verify(queryStore)
        .searchJobs(
            argThat(
                filter ->
                    filter != null
                        && filter.businessKey() == null
                        && filter.statuses() == null
                        && filter.types() == null),
            eq(10),
            eq(0));
  }

  @Test
  void findJobs_rejectsLimitLessThanOne() {
    assertThrows(
        IllegalArgumentException.class, () -> service.findJobs(JobFilter.builder().build(), 0, 0));
    assertThrows(
        IllegalArgumentException.class, () -> service.findJobs(JobFilter.builder().build(), -1, 0));

    verify(queryStore, never()).searchJobs(any(), anyInt(), anyInt());
  }

  @Test
  void findJobs_hasMore_whenMoreResultsExist() {
    when(queryStore.searchJobs(any(), eq(2), eq(0)))
        .thenReturn(List.of(minimalJob(), minimalJob()));
    when(queryStore.countJobs(any())).thenReturn(5L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 2, 0);

    assertTrue(page.hasMore(), "hasMore should be true when total > offset + page size");
  }

  @Test
  void findJobs_callsFilterForPrincipal_withCurrentCaller() {
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("alice"));
    JobFilter original = JobFilter.builder().build();
    JobFilter scoped = JobFilter.builder().callerPrincipal("alice").build();
    when(authPolicy.filterForPrincipal(any(), eq("alice"))).thenReturn(scoped);
    when(queryStore.searchJobs(eq(scoped), anyInt(), anyInt())).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(eq(scoped))).thenReturn(0L);

    service.findJobs(original, 10, 0);

    verify(authPolicy).filterForPrincipal(original, "alice");
    verify(queryStore).searchJobs(eq(scoped), eq(10), eq(0));
  }

  @Test
  void findJobs_withoutAuthOrPrincipalUsesOriginalFilter() {
    DefaultJobQueryService permissive =
        new DefaultJobQueryService(
            queryStore, crudStore, executionStore, recurringJobStore, null, null);
    JobFilter filter = JobFilter.builder().businessKey("bk-1").build();
    when(queryStore.searchJobs(eq(filter), eq(10), eq(0))).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(eq(filter))).thenReturn(0L);

    JobPage<JobSummary> page = permissive.findJobs(filter, 10, 0);

    assertTrue(page.items().isEmpty());
    verify(queryStore).searchJobs(eq(filter), eq(10), eq(0));
  }

  @Test
  void findJobs_recurringOnly_appliesPrincipalScopingInMemory() {
    // Two recurring masters owned by different principals.
    var aliceMaster = recurringDefWithPrincipal(UUID.randomUUID(), "alice");
    var bobMaster = recurringDefWithPrincipal(UUID.randomUUID(), "bob");
    when(recurringJobStore.listAll())
        .thenReturn(new java.util.ArrayList<>(List.of(aliceMaster, bobMaster)));

    // Auth policy scopes the query to the current caller (bob).
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("bob"));
    when(authPolicy.filterForPrincipal(any(), eq("bob")))
        .thenAnswer(
            inv -> ((JobFilter) inv.getArgument(0)).toBuilder().callerPrincipal("bob").build());

    JobPage<JobSummary> page =
        service.findJobs(JobFilter.builder().types(JobType.RECURRING).build(), 10, 0);

    // alice's master must NOT appear in bob's view.
    assertEquals(1, page.items().size(), "principal scoping must filter the recurring listing");
    assertEquals(bobMaster.id(), page.items().get(0).id());
  }

  @Test
  void findJobs_recurringOnly_appliesBusinessKeyFilterInMemory() {
    var matching = recurringDefWithBusinessKey(UUID.randomUUID(), "bk-keep");
    var skipped = recurringDefWithBusinessKey(UUID.randomUUID(), "bk-drop");
    when(recurringJobStore.listAll())
        .thenReturn(new java.util.ArrayList<>(List.of(matching, skipped)));

    JobPage<JobSummary> page =
        service.findJobs(
            JobFilter.builder().types(JobType.RECURRING).businessKey("bk-keep").build(), 10, 0);

    assertEquals(1, page.items().size());
    assertEquals(matching.id(), page.items().get(0).id());
  }

  @Test
  void findJobs_scopedFilterPassedToCountJobs() {
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("bob"));
    JobFilter scoped = JobFilter.builder().callerPrincipal("bob").build();
    when(authPolicy.filterForPrincipal(any(), eq("bob"))).thenReturn(scoped);
    when(queryStore.searchJobs(eq(scoped), anyInt(), anyInt())).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(eq(scoped))).thenReturn(0L);

    service.findJobs(JobFilter.builder().build(), 10, 0);

    verify(queryStore).countJobs(scoped);
  }

  @Test
  void findJobs_skipCount_skipsCountCall_returnsTotalMinusOne() {
    // skipCount=true → store is called with limit+1=11; return 1 item (partial page)
    when(queryStore.searchJobs(any(), eq(11), eq(0))).thenReturn(List.of(minimalJob()));

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().skipCount(true).build(), 10, 0);

    verify(queryStore, never()).countJobs(any());
    assertEquals(-1L, page.totalCount());
  }

  @Test
  void findJobs_skipCount_hasMore_whenPageFull() {
    // limit=2 → fetch 3; returning 3 items means hasMore=true and page returns only first 2
    List<JobEntity> overFull = List.of(minimalJob(), minimalJob(), minimalJob());
    when(queryStore.searchJobs(any(), eq(3), eq(0))).thenReturn(overFull);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().skipCount(true).build(), 2, 0);

    assertTrue(page.hasMore(), "hasMore should be true when store returns limit+1 items");
    assertEquals(2, page.items().size(), "page should contain only limit items");
  }

  @Test
  void findJobs_skipCount_noMore_whenPagePartial() {
    // limit=10 → fetch 11; returning 1 item means partial page, hasMore=false
    when(queryStore.searchJobs(any(), eq(11), eq(0))).thenReturn(List.of(minimalJob()));

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().skipCount(true).build(), 10, 0);

    assertFalse(page.hasMore(), "hasMore should be false when fewer items than limit");
  }

  // ── getJobDetail ────────────────────────────────────────────────────────

  @Test
  void findJobs_cursorModeUsesLimitPlusOneProbeForHasMoreEvenWhenCountAllowed() {
    JobFilter filter = JobFilter.builder().cursor("opaque-cursor").build();
    List<JobEntity> overFull = List.of(minimalJob(), minimalJob(), minimalJob());
    when(queryStore.searchJobs(eq(filter), eq(3), eq(0))).thenReturn(overFull);

    JobPage<JobSummary> page = service.findJobs(filter, 2, 0);

    verify(queryStore, never()).countJobs(any());
    assertEquals(-1L, page.totalCount());
    assertTrue(page.hasMore(), "cursor pages should use the limit+1 probe row for hasMore");
    assertEquals(2, page.items().size(), "cursor page should trim the probe row");
  }

  @Test
  void findJobs_nextCursor_encodesLastRowSortValueAndIdWhenHasMore() {
    UUID firstId = new UUID(0L, 101L);
    UUID secondId = new UUID(0L, 102L);
    Instant firstCreated = Instant.parse("2026-05-10T12:00:00Z");
    Instant secondCreated = Instant.parse("2026-05-10T12:01:00Z");
    JobEntity first = minimalJobWithId(firstId);
    first.setCreatedAt(firstCreated);
    JobEntity second = minimalJobWithId(secondId);
    second.setCreatedAt(secondCreated);
    when(queryStore.searchJobs(any(), eq(2), eq(0))).thenReturn(List.of(first, second));
    when(queryStore.countJobs(any())).thenReturn(5L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 2, 0);

    JobQueryCursor cursor = JobQueryCursor.decode(page.nextCursor());
    assertEquals(JobQuerySortField.CREATED_AT, cursor.sortField());
    assertEquals(secondCreated.toString(), cursor.sortValue());
    assertEquals(secondId, cursor.jobId());
  }

  @Test
  void findJobs_nextCursor_nullWhenNoMore() {
    when(queryStore.searchJobs(any(), eq(10), eq(0))).thenReturn(List.of(minimalJob()));
    when(queryStore.countJobs(any())).thenReturn(1L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 10, 0);

    assertNull(page.nextCursor(), "nextCursor should be null when no more results");
  }

  @Test
  void getJobDetail_unknownId_returnsEmpty() {
    when(crudStore.findById(any())).thenReturn(Optional.empty());

    Optional<JobDetail> result = service.getJobDetail(UUID.randomUUID());

    assertTrue(result.isEmpty());
  }

  // ── getQueueHealth ──────────────────────────────────────────────────────

  @Test
  void getJobDetail_callsCheckRead_withCallerPrincipal() throws JobAuthorizationException {
    UUID jobId = UUID.randomUUID();
    JobEntity entity = minimalJobWithId(jobId);
    when(crudStore.findById(jobId)).thenReturn(Optional.of(entity));
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("alice"));
    when(executionStore.findExecutionsByJobId(eq(jobId), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());
    when(crudStore.findDependants(eq(jobId), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());

    service.getJobDetail(jobId);

    verify(authPolicy).checkRead(jobId, "alice");
  }

  // ── getDependants ───────────────────────────────────────────────────────

  @Test
  void getJobDetail_checkReadThrows_returnsEmpty() throws JobAuthorizationException {
    UUID jobId = UUID.randomUUID();
    when(crudStore.findById(jobId)).thenReturn(Optional.of(minimalJobWithId(jobId)));
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("eve"));
    Mockito.doThrow(new JobAuthorizationException(jobId, "read", "eve", "denied"))
        .when(authPolicy)
        .checkRead(jobId, "eve");

    Optional<JobDetail> result = service.getJobDetail(jobId);

    assertTrue(result.isEmpty(), "getJobDetail should return empty when checkRead denies access");
  }

  @Test
  void getJobDetail_success_includesExecutionHistoryAndDependants() {
    UUID jobId = UUID.randomUUID();
    when(crudStore.findById(jobId)).thenReturn(Optional.of(minimalJobWithId(jobId)));
    when(principalProvider.currentPrincipal()).thenReturn(Optional.empty());
    when(executionStore.findExecutionsByJobId(eq(jobId), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());
    when(crudStore.findDependants(eq(jobId), anyInt(), anyInt())).thenReturn(List.of(minimalJob()));

    Optional<JobDetail> result = service.getJobDetail(jobId);

    assertTrue(result.isPresent());
    assertEquals(1, result.get().dependantJobIds().size());
  }

  @Test
  void getJobDetail_withoutAuthOrPrincipalReturnsDetail() {
    DefaultJobQueryService permissive =
        new DefaultJobQueryService(
            queryStore, crudStore, executionStore, recurringJobStore, null, null);
    UUID jobId = UUID.randomUUID();
    when(crudStore.findById(jobId)).thenReturn(Optional.of(minimalJobWithId(jobId)));
    when(executionStore.findExecutionsByJobId(eq(jobId), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());
    when(crudStore.findDependants(eq(jobId), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());

    Optional<JobDetail> result = permissive.getJobDetail(jobId);

    assertTrue(result.isPresent());
    assertEquals(jobId, result.get().summary().id());
    verify(authPolicy, never()).checkRead(any(), any());
  }

  @Test
  void getExecutionHistory_returnsPaginatedPage() {
    UUID jobId = UUID.randomUUID();
    UUID executionId = UUID.randomUUID();
    Instant startedAt = Instant.parse("2026-05-07T12:00:00Z");
    Instant endedAt = Instant.parse("2026-05-07T12:00:42Z");
    JobExecutionEntity execution = new JobExecutionEntity();
    execution.setId(executionId);
    execution.setJobId(jobId);
    execution.setAttempt(2);
    execution.setNodeId("node-1");
    execution.setStartedAt(startedAt);
    execution.setEndedAt(endedAt);
    execution.setDurationMs(42_000L);
    execution.setStatus(JobExecutionEntity.ExecutionStatus.SUCCEEDED);

    when(executionStore.findExecutionsByJobId(jobId, 2, 4)).thenReturn(List.of(execution));
    when(executionStore.countExecutionAttempts(jobId)).thenReturn(6);

    JobPage<ExecutionHistorySummary> history = service.getExecutionHistory(jobId, 2, 4);

    assertEquals(1, history.items().size());
    assertEquals(6L, history.totalCount());
    assertTrue(history.hasMore());
    ExecutionHistorySummary summary = history.items().get(0);
    assertEquals(executionId, summary.id());
    assertEquals(jobId, summary.jobId());
    assertEquals(2, summary.attempt());
    assertEquals("node-1", summary.nodeId());
    assertEquals(startedAt, summary.startedAt());
    assertEquals(endedAt, summary.endedAt());
    assertEquals(42_000L, summary.durationMs());
    assertTrue(summary.succeeded());
    assertNull(summary.errorMessage());
    assertNull(summary.errorClass());
    verify(executionStore).findExecutionsByJobId(jobId, 2, 4);
  }

  @Test
  void getQueueHealth_aggregatesAllCountMethods() {
    Instant oldestPending = Instant.parse("2026-05-07T10:15:30Z");
    when(crudStore.countJobsByStatuses())
        .thenReturn(
            Map.of(
                JobStatus.PENDING, 5L,
                JobStatus.RUNNING, 4L,
                JobStatus.FAILED, 3L,
                JobStatus.SUCCEEDED, 2L,
                JobStatus.CANCELED, 1L,
                JobStatus.PAUSED, 6L,
                JobStatus.WAITING, 9L));
    when(crudStore.countStuckJobs(any())).thenReturn(1L);
    when(crudStore.countReadyJobs(any())).thenReturn(3L);
    when(crudStore.getRetryRateStats(any())).thenReturn(0.1);
    when(crudStore.getAverageProcessingTime(any())).thenReturn(250.0);
    when(crudStore.getQueueWaitTimePercentile(0.95)).thenReturn(500L);
    when(crudStore.getOldestPendingJobTime()).thenReturn(Optional.of(oldestPending));
    when(crudStore.countPendingJobsByTypes())
        .thenReturn(Map.of(JobExecutionType.SINGLE, 2L, JobExecutionType.BATCH_CHILD, 3L));
    when(crudStore.countPendingJobsByPriorities())
        .thenReturn(Map.of(JobPriority.HIGH, 7L, JobPriority.CRITICAL, 8L));

    QueueHealthSnapshot snapshot = service.getQueueHealth();

    assertEquals(5L, snapshot.pendingCount());
    assertEquals(4L, snapshot.runningCount());
    assertEquals(3L, snapshot.failedCount());
    assertEquals(2L, snapshot.succeededCount());
    assertEquals(1L, snapshot.canceledCount());
    assertEquals(6L, snapshot.pausedCount());
    assertEquals(9L, snapshot.waitingCount());
    assertEquals(1L, snapshot.stuckCount());
    assertEquals(3L, snapshot.readyCount());
    assertEquals(0.1, snapshot.retryRate());
    assertEquals(250.0, snapshot.avgProcessingTimeMs());
    assertEquals(500L, snapshot.p95QueueWaitMs());
    assertEquals(oldestPending, snapshot.oldestPendingJobTime());
    assertEquals(Map.of(JobType.SINGLE, 2L, JobType.BATCH, 3L), snapshot.pendingByType());
    assertEquals(
        Map.of(JobPriority.HIGH, 7L, JobPriority.CRITICAL, 8L), snapshot.pendingByPriority());
    verify(crudStore).countJobsByStatuses();
    verify(crudStore, never()).countJobsByStatus(any());
    verify(crudStore, never()).countPendingJobsByType(any());
    verify(crudStore, never()).countPendingJobsByPriority(any());
  }

  @Test
  void getQueueHealth_usesInjectedClockForTimeWindows() {
    when(crudStore.countJobsByStatuses()).thenReturn(Map.of());
    when(crudStore.countPendingJobsByTypes()).thenReturn(Map.of());
    when(crudStore.countPendingJobsByPriorities()).thenReturn(Map.of());
    when(crudStore.countStuckJobs(any())).thenReturn(0L);
    when(crudStore.countReadyJobs(any())).thenReturn(0L);
    when(crudStore.getRetryRateStats(any())).thenReturn(0.0);
    when(crudStore.getAverageProcessingTime(any())).thenReturn(0.0);
    when(crudStore.getOldestPendingJobTime()).thenReturn(Optional.empty());

    service.getQueueHealth();

    verify(crudStore).countStuckJobs(FIXED_NOW.minusSeconds(300));
    verify(crudStore).countReadyJobs(FIXED_NOW);
    verify(crudStore).getRetryRateStats(FIXED_NOW.minusSeconds(3600));
    verify(crudStore).getAverageProcessingTime(FIXED_NOW.minusSeconds(3600));
  }

  // ── helpers ─────────────────────────────────────────────────────────────

  @Test
  void getDependants_returnsPaginatedPage() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = minimalJob();
    when(queryStore.searchJobs(
            argThat(filter -> parentId.equals(filter.parentJobId())), eq(2), eq(3)))
        .thenReturn(List.of(child));
    when(queryStore.countJobs(any())).thenReturn(4L);

    JobPage<JobSummary> result = service.getDependants(parentId, 2, 3);

    assertEquals(1, result.items().size());
    assertEquals(child.getId(), result.items().get(0).id());
    assertEquals(4L, result.totalCount());
    assertFalse(result.hasMore());
  }

  @Test
  void getBatchChildren_returnsPaginatedPage() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = minimalJob();
    when(queryStore.searchJobs(
            argThat(
                filter -> parentId.equals(filter.parentJobId()) && hasType(filter, JobType.BATCH)),
            eq(2),
            eq(4)))
        .thenReturn(List.of(child));
    when(queryStore.countJobs(any())).thenReturn(5L);

    JobPage<JobSummary> page = service.getBatchChildren(parentId, 2, 4);

    assertEquals(1, page.items().size());
    assertEquals(child.getId(), page.items().get(0).id());
    assertEquals(5L, page.totalCount());
    assertFalse(page.hasMore());
  }

  @Test
  void getRecurringMasters_returnsPaginatedPage() {
    List<run.ratchet.store.spi.RecurringJobDefinition> masters =
        new java.util.ArrayList<>(
            List.of(recurringDefinition("0 * * * * ?"), recurringDefinition("0 0 * * * ?")));
    masters.add(recurringDefinition("0 0 0 * * ?"));
    when(recurringJobStore.listAll()).thenReturn(masters);

    JobPage<JobSummary> page = service.getRecurringMasters(2, 0);

    assertEquals(2, page.items().size());
    assertEquals(3L, page.totalCount());
    assertTrue(page.hasMore());
    verify(queryStore, never()).searchJobs(any(), anyInt(), anyInt());
  }

  private static run.ratchet.store.spi.RecurringJobDefinition recurringDefinition(String cron) {
    return new run.ratchet.store.spi.RecurringJobDefinition(
        UUID.randomUUID(),
        cron,
        "UTC",
        Instant.parse("2026-05-20T12:00:00Z"),
        false,
        null,
        run.ratchet.api.JobPriority.NORMAL.ordinal(),
        0,
        run.ratchet.api.BackoffPolicy.NONE,
        0,
        0,
        new run.ratchet.store.entity.JobPayload(
            "com.example.Recurring", "tick", "()V", true, List.of()),
        null,
        null,
        null,
        null,
        null,
        Instant.parse("2026-05-19T00:00:00Z"),
        null);
  }

  private static run.ratchet.store.spi.RecurringJobDefinition recurringDefWithPrincipal(
      UUID id, String principal) {
    return new run.ratchet.store.spi.RecurringJobDefinition(
        id,
        "0 * * * * ?",
        "UTC",
        Instant.parse("2026-05-20T12:00:00Z"),
        false,
        null,
        run.ratchet.api.JobPriority.NORMAL.ordinal(),
        0,
        run.ratchet.api.BackoffPolicy.NONE,
        0,
        0,
        new run.ratchet.store.entity.JobPayload(
            "com.example.Recurring", "tick", "()V", true, List.of()),
        null,
        null,
        null,
        null,
        null,
        Instant.parse("2026-05-19T00:00:00Z"),
        principal);
  }

  private static run.ratchet.store.spi.RecurringJobDefinition recurringDefWithBusinessKey(
      UUID id, String businessKey) {
    return new run.ratchet.store.spi.RecurringJobDefinition(
        id,
        "0 * * * * ?",
        "UTC",
        Instant.parse("2026-05-20T12:00:00Z"),
        false,
        null,
        run.ratchet.api.JobPriority.NORMAL.ordinal(),
        0,
        run.ratchet.api.BackoffPolicy.NONE,
        0,
        0,
        new run.ratchet.store.entity.JobPayload(
            "com.example.Recurring", "tick", "()V", true, List.of()),
        null,
        null,
        businessKey,
        null,
        null,
        Instant.parse("2026-05-19T00:00:00Z"),
        null);
  }
}
