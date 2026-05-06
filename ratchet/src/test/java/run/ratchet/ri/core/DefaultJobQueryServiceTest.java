package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobDetail;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;
import run.ratchet.api.JobType;
import run.ratchet.api.QueueHealthSnapshot;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobQueryStore;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultJobQueryServiceTest {

  @Mock private JobQueryStore queryStore;
  @Mock private JobCrudStore crudStore;
  @Mock private ExecutionStore executionStore;
  @Mock private JobAuthorizationPolicy authPolicy;
  @Mock private CallerPrincipalProvider principalProvider;

  private DefaultJobQueryService service;

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobQueryService(
            queryStore, crudStore, executionStore, authPolicy, principalProvider);
    lenient().when(principalProvider.currentPrincipal()).thenReturn(Optional.empty());
    lenient()
        .when(authPolicy.filterForPrincipal(any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));
  }

  // ── findJobs ────────────────────────────────────────────────────────────

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
        new DefaultJobQueryService(queryStore, crudStore, executionStore, null, null);
    JobFilter filter = JobFilter.builder().businessKey("bk-1").build();
    when(queryStore.searchJobs(eq(filter), eq(10), eq(0))).thenReturn(Collections.emptyList());
    when(queryStore.countJobs(eq(filter))).thenReturn(0L);

    JobPage<JobSummary> page = permissive.findJobs(filter, 10, 0);

    assertTrue(page.items().isEmpty());
    verify(queryStore).searchJobs(eq(filter), eq(10), eq(0));
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
  void findJobs_nextCursor_setWhenHasMore() {
    when(queryStore.searchJobs(any(), eq(2), eq(0)))
        .thenReturn(List.of(minimalJob(), minimalJob()));
    when(queryStore.countJobs(any())).thenReturn(5L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 2, 0);

    assertNotNull(page.nextCursor(), "nextCursor should be set when hasMore");
  }

  @Test
  void findJobs_nextCursor_nullWhenNoMore() {
    when(queryStore.searchJobs(any(), eq(10), eq(0))).thenReturn(List.of(minimalJob()));
    when(queryStore.countJobs(any())).thenReturn(1L);

    JobPage<JobSummary> page = service.findJobs(JobFilter.builder().build(), 10, 0);

    assertNull(page.nextCursor(), "nextCursor should be null when no more results");
  }

  // ── getJobDetail ────────────────────────────────────────────────────────

  @Test
  void getJobDetail_unknownId_returnsEmpty() {
    when(crudStore.findById(any())).thenReturn(Optional.empty());

    Optional<JobDetail> result = service.getJobDetail(UUID.randomUUID());

    assertTrue(result.isEmpty());
  }

  @Test
  void getJobDetail_callsCheckRead_withCallerPrincipal() throws JobAuthorizationException {
    UUID jobId = UUID.randomUUID();
    JobEntity entity = minimalJobWithId(jobId);
    when(crudStore.findById(jobId)).thenReturn(Optional.of(entity));
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("alice"));
    when(executionStore.findExecutionsByJobId(jobId)).thenReturn(Collections.emptyList());
    when(crudStore.findDependants(jobId)).thenReturn(Collections.emptyList());

    service.getJobDetail(jobId);

    verify(authPolicy).checkRead(jobId, "alice");
  }

  @Test
  void getJobDetail_checkReadThrows_returnsEmpty() throws JobAuthorizationException {
    UUID jobId = UUID.randomUUID();
    when(crudStore.findById(jobId)).thenReturn(Optional.of(minimalJobWithId(jobId)));
    when(principalProvider.currentPrincipal()).thenReturn(Optional.of("eve"));
    org.mockito.Mockito.doThrow(new JobAuthorizationException(jobId, "read", "eve", "denied"))
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
    when(executionStore.findExecutionsByJobId(jobId)).thenReturn(Collections.emptyList());
    when(crudStore.findDependants(jobId)).thenReturn(List.of(minimalJob()));

    Optional<JobDetail> result = service.getJobDetail(jobId);

    assertTrue(result.isPresent());
    assertEquals(1, result.get().dependantJobIds().size());
  }

  // ── getQueueHealth ──────────────────────────────────────────────────────

  @Test
  void getQueueHealth_aggregatesAllCountMethods() {
    when(crudStore.countJobsByStatus(any())).thenReturn(5L);
    when(crudStore.countStuckJobs(any())).thenReturn(1L);
    when(crudStore.countReadyJobs(any())).thenReturn(3L);
    when(crudStore.getRetryRateStats(any())).thenReturn(0.1);
    when(crudStore.getAverageProcessingTime(any())).thenReturn(250.0);
    when(crudStore.getQueueWaitTimePercentile(0.95)).thenReturn(500L);
    when(crudStore.getOldestPendingJobTime()).thenReturn(Optional.empty());
    when(crudStore.countPendingJobsByType(any())).thenReturn(0L);
    when(crudStore.countPendingJobsByPriority(any())).thenReturn(0L);

    QueueHealthSnapshot snapshot = service.getQueueHealth();

    assertEquals(5L, snapshot.pendingCount());
    assertEquals(1L, snapshot.stuckCount());
    assertEquals(3L, snapshot.readyCount());
    assertEquals(0.1, snapshot.retryRate());
    assertEquals(500L, snapshot.p95QueueWaitMs());
  }

  // ── getDependants ───────────────────────────────────────────────────────

  @Test
  void getDependants_delegatesToCrudStore() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = minimalJob();
    when(crudStore.findDependants(parentId)).thenReturn(List.of(child));

    List<JobSummary> result = service.getDependants(parentId);

    assertEquals(1, result.size());
    assertEquals(child.getId(), result.get(0).id());
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
    List<JobEntity> masters = List.of(minimalJob(), minimalJob());
    when(queryStore.searchJobs(argThat(filter -> hasType(filter, JobType.RECURRING)), eq(2), eq(0)))
        .thenReturn(masters);
    when(queryStore.countJobs(any())).thenReturn(3L);

    JobPage<JobSummary> page = service.getRecurringMasters(2, 0);

    assertEquals(2, page.items().size());
    assertEquals(3L, page.totalCount());
    assertTrue(page.hasMore());
  }

  // ── helpers ─────────────────────────────────────────────────────────────

  private static boolean hasType(JobFilter filter, JobType type) {
    return filter != null && filter.types() != null && filter.types().contains(type);
  }

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
}
