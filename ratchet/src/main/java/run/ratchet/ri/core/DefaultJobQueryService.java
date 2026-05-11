package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import run.ratchet.api.ExecutionHistorySummary;
import run.ratchet.api.JobDetail;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;
import run.ratchet.api.JobType;
import run.ratchet.api.QueueHealthSnapshot;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.query.JobQueryCursor;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobQueryStore;

/** Default {@link JobQueryService} implementation backed by the store SPI. */
@ApplicationScoped
public class DefaultJobQueryService implements JobQueryService {

  private final JobQueryStore queryStore;
  private final JobCrudStore crudStore;
  private final ExecutionStore executionStore;
  private final JobAuthorizationPolicy authPolicy;
  private final CallerPrincipalProvider principalProvider;

  protected DefaultJobQueryService() {
    this.queryStore = null;
    this.crudStore = null;
    this.executionStore = null;
    this.authPolicy = null;
    this.principalProvider = null;
  }

  @Inject
  public DefaultJobQueryService(
      JobQueryStore queryStore,
      JobCrudStore crudStore,
      ExecutionStore executionStore,
      JobAuthorizationPolicy authPolicy,
      CallerPrincipalProvider principalProvider) {
    this.queryStore = queryStore;
    this.crudStore = crudStore;
    this.executionStore = executionStore;
    this.authPolicy = authPolicy;
    this.principalProvider = principalProvider;
  }

  private static String extractSortValue(JobEntity last, JobQuerySortField field) {
    return switch (field) {
      case CREATED_AT -> toInstantString(last.getCreatedAt());
      case SCHEDULED_TIME -> {
        Instant t = last.getScheduledTime();
        if (t == null) t = last.getExecutionStartTime();
        if (t == null) t = last.getCreatedAt();
        yield toInstantString(t);
      }
      case UPDATED_AT -> {
        Instant t = last.getUpdatedAt();
        if (t == null) t = last.getCreatedAt();
        yield toInstantString(t);
      }
      case PRIORITY ->
          String.valueOf(last.getPriority() != null ? last.getPriority().ordinal() : 0);
      case STATUS -> last.getStatus() != null ? last.getStatus().name() : JobStatus.PENDING.name();
    };
  }

  private static String toInstantString(Instant t) {
    return t != null ? t.toString() : Instant.EPOCH.toString();
  }

  @Override
  public JobPage<JobSummary> findJobs(JobFilter filter, int limit, int offset) {
    validatePageRequest(limit, offset);

    JobFilter scoped = scopeFilter(filter);

    boolean cursorMode = scoped.cursor() != null && !scoped.cursor().isBlank();
    boolean probeMode = scoped.skipCount() || cursorMode;
    int fetchLimit = probeMode ? limit + 1 : limit;
    List<JobEntity> rows = queryStore.searchJobs(scoped, fetchLimit, offset);

    long total;
    boolean hasMore;
    if (probeMode) {
      hasMore = rows.size() > limit;
      total = -1L;
      if (hasMore) {
        rows = rows.subList(0, limit);
      }
    } else {
      total = queryStore.countJobs(scoped);
      hasMore = (long) offset + rows.size() < total;
    }

    List<JobSummary> items =
        rows.stream().map(JobEntityMapper::toSummary).collect(Collectors.toList());

    String nextCursor = null;
    if (hasMore && !rows.isEmpty()) {
      JobEntity last = rows.get(rows.size() - 1);
      JobQuerySortField sortField =
          scoped.sortField() != null ? scoped.sortField() : JobQuerySortField.CREATED_AT;
      nextCursor =
          new JobQueryCursor(sortField, extractSortValue(last, sortField), last.getId()).encode();
    }

    return new JobPage<>(items, total, limit, offset, hasMore, nextCursor);
  }

  @Override
  public Optional<JobDetail> getJobDetail(UUID jobId) {
    Optional<JobEntity> opt = crudStore.findById(jobId);
    if (opt.isEmpty()) {
      return Optional.empty();
    }
    JobEntity e = opt.get();
    if (authPolicy != null) {
      try {
        authPolicy.checkRead(jobId, currentPrincipal());
      } catch (JobAuthorizationException ex) {
        return Optional.empty();
      }
    }

    List<ExecutionHistorySummary> history =
        executionStore.findExecutionsByJobId(jobId, DEFAULT_PAGE_LIMIT, 0).stream()
            .map(JobEntityMapper::toExecutionSummary)
            .collect(Collectors.toList());

    List<UUID> dependantIds =
        crudStore.findDependants(jobId, DEFAULT_PAGE_LIMIT, 0).stream()
            .map(JobEntity::getId)
            .collect(Collectors.toList());

    JobDetail detail =
        new JobDetail(
            JobEntityMapper.toSummary(e),
            e.getParams(),
            e.getTraceContext(),
            e.getJobResult(),
            e.getResultType(),
            e.getExecutionStartTime(),
            e.getExecutionEndTime(),
            e.getExecutionDurationMs(),
            e.getQueueWaitMs(),
            history,
            dependantIds);
    return Optional.of(detail);
  }

  @Override
  public JobPage<ExecutionHistorySummary> getExecutionHistory(UUID jobId, int limit, int offset) {
    validatePageRequest(limit, offset);
    List<ExecutionHistorySummary> items =
        executionStore.findExecutionsByJobId(jobId, limit, offset).stream()
            .map(JobEntityMapper::toExecutionSummary)
            .collect(Collectors.toList());
    long total = executionStore.countExecutionAttempts(jobId);
    boolean hasMore = (long) offset + items.size() < total;
    return new JobPage<>(items, total, limit, offset, hasMore, null);
  }

  @Override
  public QueueHealthSnapshot getQueueHealth() {
    Instant now = Instant.now();
    Instant stuckThreshold = now.minusSeconds(300);
    Instant since = now.minusSeconds(3600);

    Map<JobType, Long> pendingByType = new EnumMap<>(JobType.class);
    crudStore
        .countPendingJobsByTypes()
        .forEach((type, count) -> pendingByType.merge(type.toPublicType(), count, Long::sum));

    Map<JobPriority, Long> pendingByPriority = new EnumMap<>(JobPriority.class);
    pendingByPriority.putAll(crudStore.countPendingJobsByPriorities());
    Map<JobStatus, Long> countsByStatus = crudStore.countJobsByStatuses();

    return new QueueHealthSnapshot(
        countsByStatus.getOrDefault(JobStatus.PENDING, 0L),
        countsByStatus.getOrDefault(JobStatus.RUNNING, 0L),
        countsByStatus.getOrDefault(JobStatus.FAILED, 0L),
        countsByStatus.getOrDefault(JobStatus.SUCCEEDED, 0L),
        countsByStatus.getOrDefault(JobStatus.CANCELED, 0L),
        countsByStatus.getOrDefault(JobStatus.PAUSED, 0L),
        crudStore.countStuckJobs(stuckThreshold),
        crudStore.countReadyJobs(now),
        crudStore.getRetryRateStats(since),
        crudStore.getAverageProcessingTime(since),
        crudStore.getQueueWaitTimePercentile(0.95),
        crudStore.getOldestPendingJobTime().orElse(null),
        pendingByType,
        pendingByPriority);
  }

  @Override
  public JobPage<JobSummary> getDependants(UUID jobId, int limit, int offset) {
    return findJobs(JobFilter.builder().parentJobId(jobId).build(), limit, offset);
  }

  @Override
  public JobPage<JobSummary> getBatchChildren(UUID batchParentId, int limit, int offset) {
    return findJobs(
        JobFilter.builder().parentJobId(batchParentId).types(JobType.BATCH).build(), limit, offset);
  }

  @Override
  public JobPage<JobSummary> getRecurringMasters(int limit, int offset) {
    return findJobs(JobFilter.builder().types(JobType.RECURRING).build(), limit, offset);
  }

  private static void validatePageRequest(int limit, int offset) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be at least 1");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be non-negative");
    }
  }

  private JobFilter scopeFilter(JobFilter filter) {
    JobFilter original = filter != null ? filter : JobFilter.builder().build();
    if (authPolicy == null) {
      return original;
    }
    JobFilter scoped = authPolicy.filterForPrincipal(original, currentPrincipal());
    return scoped != null ? scoped : original;
  }

  private String currentPrincipal() {
    return principalProvider != null ? principalProvider.currentPrincipal().orElse(null) : null;
  }
}
