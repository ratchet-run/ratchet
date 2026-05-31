package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
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
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.PayloadMasker;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.query.JobQueryCursor;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

/** Default {@link JobQueryService} implementation backed by the store SPI. */
@ApplicationScoped
class DefaultJobQueryService implements JobQueryService {

  private final JobQueryStore queryStore;
  private final JobCrudStore crudStore;
  private final ExecutionStore executionStore;
  private final RecurringJobStore recurringJobStore;
  private final JobAuthorizationPolicy authPolicy;
  private final CallerPrincipalProvider principalProvider;
  private final Clock clock;
  private final boolean maskPayloads;

  protected DefaultJobQueryService() {
    this.queryStore = null;
    this.crudStore = null;
    this.executionStore = null;
    this.recurringJobStore = null;
    this.authPolicy = null;
    this.principalProvider = null;
    this.clock = null;
    this.maskPayloads = false;
  }

  public DefaultJobQueryService(
      JobQueryStore queryStore,
      JobCrudStore crudStore,
      ExecutionStore executionStore,
      RecurringJobStore recurringJobStore,
      JobAuthorizationPolicy authPolicy,
      CallerPrincipalProvider principalProvider) {
    this(
        queryStore,
        crudStore,
        executionStore,
        recurringJobStore,
        authPolicy,
        principalProvider,
        Clock.systemUTC());
  }

  public DefaultJobQueryService(
      JobQueryStore queryStore,
      JobCrudStore crudStore,
      ExecutionStore executionStore,
      RecurringJobStore recurringJobStore,
      JobAuthorizationPolicy authPolicy,
      CallerPrincipalProvider principalProvider,
      Clock clock) {
    this(
        queryStore,
        crudStore,
        executionStore,
        recurringJobStore,
        authPolicy,
        principalProvider,
        clock,
        null);
  }

  @Inject
  public DefaultJobQueryService(
      JobQueryStore queryStore,
      JobCrudStore crudStore,
      ExecutionStore executionStore,
      RecurringJobStore recurringJobStore,
      JobAuthorizationPolicy authPolicy,
      CallerPrincipalProvider principalProvider,
      Clock clock,
      RatchetOptions options) {
    this.queryStore = queryStore;
    this.crudStore = crudStore;
    this.executionStore = executionStore;
    this.recurringJobStore = recurringJobStore;
    this.authPolicy = authPolicy;
    this.principalProvider = principalProvider;
    this.clock = clock;
    this.maskPayloads = options != null && options.security().maskPayloads();
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

    // A filter that targets ONLY recurring masters has to hit RecurringJobStore — the executable
    // queryStore reads scheduler_job + scheduler_job_queue, which don't hold recurring rows. The
    // scoped filter includes any principal-scoping the auth policy injected, so we apply it in-
    // memory against the master rows rather than dropping straight to listAll.
    if (scoped.types() != null
        && scoped.types().size() == 1
        && scoped.types().contains(JobType.RECURRING)) {
      return findRecurringMastersWithFilter(scoped, limit, offset);
    }

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

    // Read-projection masking: getJobDetail exposes the raw caller-supplied params, the trace
    // context carrier (whose baggage entries can hold caller data), and the serialized job result.
    // When mask-payloads is enabled we redact sensitive entries here before they reach the caller;
    // the durable row is untouched and the summary's target#method stays unmasked. Map masking is
    // key-based and result masking walks the serialized JSON object (a non-object result passes
    // through). Free-text fields such as lastError are out of scope — see
    // RatchetOptions.SecurityOptions#maskPayloads.
    Map<String, String> params =
        maskPayloads ? PayloadMasker.maskParams(e.getParams()) : e.getParams();
    Map<String, String> traceContext =
        maskPayloads ? PayloadMasker.maskParams(e.getTraceContext()) : e.getTraceContext();
    String jobResult =
        maskPayloads ? PayloadMasker.maskPayload(e.getJobResult()) : e.getJobResult();

    JobDetail detail =
        new JobDetail(
            JobEntityMapper.toSummary(e),
            params,
            traceContext,
            jobResult,
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
    if (authPolicy != null) {
      try {
        authPolicy.checkRead(jobId, currentPrincipal());
      } catch (JobAuthorizationException ex) {
        // Empty-on-denial — same contract as getJobDetail. Returning an exception would leak
        // existence; an empty page is indistinguishable from a job with no execution history.
        return new JobPage<>(List.of(), 0L, limit, offset, false, null);
      }
    }
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
    Instant now = effective().instant();
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
        countsByStatus.getOrDefault(JobStatus.WAITING, 0L),
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
    validatePageRequest(limit, offset);
    // Run the public listing through the same scoped-filter pipeline as findJobs so any
    // principal-scoping injected by JobAuthorizationPolicy.filterForPrincipal is applied here
    // too — without this, getRecurringMasters would bypass cross-tenant visibility constraints.
    JobFilter scoped = scopeFilter(JobFilter.builder().types(JobType.RECURRING).build());
    return findRecurringMastersWithFilter(scoped, limit, offset);
  }

  private JobPage<JobSummary> findRecurringMastersWithFilter(
      JobFilter scoped, int limit, int offset) {
    if (recurringJobStore == null) {
      return new JobPage<>(List.<JobSummary>of(), 0L, limit, offset, false, null);
    }
    // RecurringJobStore.listAll has no native pagination; recurring-master populations are small
    // by design (one per business key) so the slice happens in memory. Sort is stable by id to
    // keep page boundaries deterministic across calls. Copy first — the SPI doesn't guarantee a
    // mutable list and we don't want to leak side effects back to the store.
    List<RecurringJobDefinition> filtered = new java.util.ArrayList<>();
    for (RecurringJobDefinition def : recurringJobStore.listAll()) {
      if (matchesRecurringFilter(def, scoped)) {
        filtered.add(def);
      }
    }
    filtered.sort((a, b) -> a.id().compareTo(b.id()));
    long total = filtered.size();
    int from = Math.min(offset, filtered.size());
    int to = Math.min(offset + limit, filtered.size());
    List<JobSummary> page = new java.util.ArrayList<>(to - from);
    for (RecurringJobDefinition def : filtered.subList(from, to)) {
      page.add(toRecurringSummary(def));
    }
    return new JobPage<>(page, total, limit, offset, to < filtered.size(), null);
  }

  private static boolean matchesRecurringFilter(RecurringJobDefinition def, JobFilter f) {
    // callerPrincipal: drives the auth-scoping case as well as caller-supplied filters.
    if (f.callerPrincipal() != null && !f.callerPrincipal().equals(def.callerPrincipal())) {
      return false;
    }
    if (f.businessKey() != null && !f.businessKey().equals(def.businessKey())) {
      return false;
    }
    if (f.resourceName() != null && !f.resourceName().equals(def.resourceName())) {
      return false;
    }
    if (f.targetClass() != null
        && (def.payload() == null || !f.targetClass().equals(def.payload().target()))) {
      return false;
    }
    if (f.createdAfter() != null
        && def.createdAt() != null
        && def.createdAt().isBefore(f.createdAfter())) {
      return false;
    }
    if (f.createdBefore() != null
        && def.createdAt() != null
        && def.createdAt().isAfter(f.createdBefore())) {
      return false;
    }
    // Filter dimensions not yet supported for recurring masters: tags, statuses, idempotencyKey,
    // priorities, pickedBy, traceCorrelationId, parentJobId, scheduledAfter/Before, updatedAfter.
    // None of these have natural meaning on a recurring master row; callers that need them on
    // executable children should query JobType.SINGLE separately.
    return true;
  }

  private static JobSummary toRecurringSummary(RecurringJobDefinition def) {
    JobPayload payload = def.payload();
    return new JobSummary(
        def.id(),
        def.paused() ? JobStatus.PAUSED : JobStatus.PENDING,
        JobType.RECURRING,
        JobPriorityMapper.fromOrdinal(def.priority()),
        def.businessKey(),
        /* idempotencyKey */ null,
        payload != null ? payload.target() : null,
        payload != null ? payload.method() : null,
        List.of(),
        def.resourceName(),
        /* pickedBy */ null,
        def.createdAt(),
        def.nextFire(),
        def.createdAt(),
        def.callerPrincipal(),
        /* lastError */ null,
        /* attempts */ 0,
        def.maxRetries(),
        /* dependsOn */ null);
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

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
