package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.query.JobQueryCursor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Dashboard-oriented search and count queries over the MongoDB job store.
 *
 * <p>All jobs live in a single {@code scheduler_job} collection (no hot/cold split). Filters are
 * composed with {@link com.mongodb.client.model.Filters} and bound to the driver — no string SQL is
 * built here. Tag filtering uses an embedded array field; ANY-of semantics are native to {@code
 * $in}.
 *
 * <p>When {@link JobFilter#includeArchived()} is true and no principal filter is active, the
 * archive collection is queried separately and results are merged in memory before applying the
 * limit. Archive documents are mapped to {@link JobEntity} using archive-specific field names; tags
 * and trace-context filtering are not applied to archived rows (those fields are absent from the
 * archive document schema). The caller-principal check is intentionally skipped for archived rows.
 */
final class MongoJobQueryOperations {

  private static final int MAX_LIMIT = 1000;

  private final MongoStoreContext ctx;

  MongoJobQueryOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  List<JobEntity> searchJobs(JobFilter filter, int limit, int offset) {
    int safeLimit = Math.min(limit, MAX_LIMIT);
    boolean archive = useArchive(filter);

    if (!archive) {
      return searchLive(filter, safeLimit, offset);
    }

    // Fetch from both collections and merge in memory
    int fetchLimit = safeLimit + offset;
    List<JobEntity> live = searchLive(filter, fetchLimit, 0);
    List<JobEntity> archived = searchArchive(filter, fetchLimit);

    List<JobEntity> merged = new ArrayList<>(live.size() + archived.size());
    merged.addAll(live);
    merged.addAll(archived);
    merged.sort(mergeComparator(filter));
    int from = Math.min(offset, merged.size());
    int to = Math.min(from + safeLimit, merged.size());
    return merged.subList(from, to);
  }

  long countJobs(JobFilter filter) {
    long liveCount = ctx.jobs().countDocuments(buildFilter(filter));
    if (!useArchive(filter)) {
      return liveCount;
    }
    long archiveCount = ctx.archives().countDocuments(buildArchiveFilter(filter));
    return liveCount + archiveCount;
  }

  // ── Live collection query ───────────────────────────────────────────────

  private List<JobEntity> searchLive(JobFilter filter, int limit, int offset) {
    Bson query = buildFilter(filter);
    Bson sort = buildSort(filter);
    List<JobEntity> result = new ArrayList<>(limit);
    for (Document doc : ctx.jobs().find(query).sort(sort).skip(offset).limit(limit)) {
      JobEntity job = DocumentMapper.toJobEntity(doc);
      if (job != null) {
        result.add(job);
      }
    }
    return result;
  }

  // ── Archive collection query ────────────────────────────────────────────

  private List<JobEntity> searchArchive(JobFilter filter, int limit) {
    Bson query = buildArchiveFilter(filter);
    Bson sort = buildArchiveSort(filter);
    List<JobEntity> result = new ArrayList<>(limit);
    for (Document doc : ctx.archives().find(query).sort(sort).limit(limit)) {
      JobEntity job = DocumentMapper.archivedDocToJobEntity(doc);
      if (job != null) {
        result.add(job);
      }
    }
    return result;
  }

  // ── Filter builders ─────────────────────────────────────────────────────

  private Bson buildFilter(JobFilter filter) {
    List<Bson> conditions = new ArrayList<>();
    if (filter == null) {
      return new Document();
    }

    appendStatusCondition(filter, conditions);
    appendJobTypeCondition(filter, conditions);
    appendPriorityCondition(filter, conditions);
    appendStringEq(MongoFieldNames.BUSINESS_KEY, filter.businessKey(), conditions);
    appendStringEq(MongoFieldNames.IDEMPOTENCY_KEY, filter.idempotencyKey(), conditions);
    appendStringEq(MongoFieldNames.TARGET_CLASS, filter.targetClass(), conditions);
    appendStringEq(MongoFieldNames.CALLER_PRINCIPAL, filter.callerPrincipal(), conditions);
    appendStringEq(MongoFieldNames.PICKED_BY, filter.pickedBy(), conditions);
    appendStringEq(MongoFieldNames.RESOURCE_NAME, filter.resourceName(), conditions);
    appendStringEq(MongoFieldNames.TRACE_CONTEXT + ".traceparent", filter.traceCorrelationId(), conditions);
    appendParentJobId(filter, conditions);
    appendTagCondition(filter, conditions);
    appendInstantGte(MongoFieldNames.CREATED_AT, filter.createdAfter(), conditions);
    appendInstantLt(MongoFieldNames.CREATED_AT, filter.createdBefore(), conditions);
    appendInstantGte(MongoFieldNames.SCHEDULED_TIME, filter.scheduledAfter(), conditions);
    appendInstantLt(MongoFieldNames.SCHEDULED_TIME, filter.scheduledBefore(), conditions);
    appendInstantGte(MongoFieldNames.UPDATED_AT, filter.updatedAfter(), conditions);
    appendCursorCondition(filter, conditions, false);

    return conditions.isEmpty() ? new Document() : and(conditions);
  }

  private Bson buildArchiveFilter(JobFilter filter) {
    List<Bson> conditions = new ArrayList<>();
    if (filter == null) {
      return new Document();
    }

    appendArchiveStatusCondition(filter, conditions);
    appendArchiveJobTypeCondition(filter, conditions);
    appendArchivePriorityCondition(filter, conditions);
    appendStringEq(MongoFieldNames.BUSINESS_KEY, filter.businessKey(), conditions);
    appendStringEq(MongoFieldNames.TARGET_CLASS, filter.targetClass(), conditions);
    appendArchiveParentJobId(filter, conditions);
    appendInstantGte(MongoFieldNames.ORIGINAL_CREATED_AT, filter.createdAfter(), conditions);
    appendInstantLt(MongoFieldNames.ORIGINAL_CREATED_AT, filter.createdBefore(), conditions);
    appendInstantGte(MongoFieldNames.SCHEDULED_TIME, filter.scheduledAfter(), conditions);
    appendInstantLt(MongoFieldNames.SCHEDULED_TIME, filter.scheduledBefore(), conditions);
    appendInstantGte(MongoFieldNames.ARCHIVED_AT, filter.updatedAfter(), conditions);
    appendCursorCondition(filter, conditions, true);

    return conditions.isEmpty() ? new Document() : and(conditions);
  }

  private static void appendStatusCondition(JobFilter filter, List<Bson> conditions) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    List<String> names = statuses.stream().map(JobStatus::name).collect(Collectors.toList());
    conditions.add(in(MongoFieldNames.STATUS, names));
  }

  private static void appendArchiveStatusCondition(JobFilter filter, List<Bson> conditions) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    // Archive only holds terminal statuses; filter to terminal subset
    List<String> terminal = statuses.stream()
        .filter(s -> s == JobStatus.SUCCEEDED || s == JobStatus.FAILED || s == JobStatus.CANCELED)
        .map(JobStatus::name)
        .collect(Collectors.toList());
    if (terminal.isEmpty()) {
      conditions.add(eq("_impossible_field_", true));
      return;
    }
    conditions.add(in(MongoFieldNames.FINAL_STATUS, terminal));
  }

  private static void appendJobTypeCondition(JobFilter filter, List<Bson> conditions) {
    Set<JobType> types = filter.types();
    if (types == null || types.isEmpty()) {
      return;
    }
    List<String> execTypeNames =
        Stream.of(JobExecutionType.values())
            .filter(e -> types.contains(e.toPublicType()))
            .map(Enum::name)
            .collect(Collectors.toList());
    if (execTypeNames.isEmpty()) {
      return;
    }
    conditions.add(in(MongoFieldNames.JOB_TYPE, execTypeNames));
  }

  private static void appendArchiveJobTypeCondition(JobFilter filter, List<Bson> conditions) {
    appendJobTypeCondition(filter, conditions); // same field name in archive
  }

  private static void appendPriorityCondition(JobFilter filter, List<Bson> conditions) {
    Set<JobPriority> priorities = filter.priorities();
    if (priorities == null || priorities.isEmpty()) {
      return;
    }
    List<Integer> ordinals =
        priorities.stream().map(JobPriority::ordinal).collect(Collectors.toList());
    conditions.add(in(MongoFieldNames.PRIORITY, ordinals));
  }

  private static void appendArchivePriorityCondition(JobFilter filter, List<Bson> conditions) {
    appendPriorityCondition(filter, conditions); // same field name in archive
  }

  private static void appendParentJobId(JobFilter filter, List<Bson> conditions) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    conditions.add(eq(MongoFieldNames.DEPENDS_ON, parentJobId));
  }

  private static void appendArchiveParentJobId(JobFilter filter, List<Bson> conditions) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    // Archive uses "depends_on" for the same concept (same field name set in DocumentMapper)
    conditions.add(eq(MongoFieldNames.DEPENDS_ON, parentJobId));
  }

  private static void appendTagCondition(JobFilter filter, List<Bson> conditions) {
    Set<String> tags = filter.tags();
    if (tags == null || tags.isEmpty()) {
      return;
    }
    // $in on an array field matches documents where the array contains any of the values
    conditions.add(in(MongoFieldNames.TAGS, tags));
  }

  private static void appendStringEq(String field, String value, List<Bson> conditions) {
    if (value == null || value.isEmpty()) {
      return;
    }
    conditions.add(eq(field, value));
  }

  private static void appendInstantGte(
      String field, java.time.Instant value, List<Bson> conditions) {
    if (value == null) {
      return;
    }
    conditions.add(gte(field, Date.from(value)));
  }

  private static void appendInstantLt(
      String field, java.time.Instant value, List<Bson> conditions) {
    if (value == null) {
      return;
    }
    conditions.add(lt(field, Date.from(value)));
  }

  private static void appendCursorCondition(
      JobFilter filter, List<Bson> conditions, boolean archive) {
    if (filter == null || filter.cursor() == null) {
      return;
    }
    try {
      JobQueryCursor c = JobQueryCursor.decode(filter.cursor());
      String field = archive ? archiveSortField(c.sortField) : sortField(c.sortField);
      Object sortVal = parseSortValue(c);
      // (field < sortVal) OR (field == sortVal AND _id > jobId)
      if (filter.sortAscending()) {
        conditions.add(or(gt(field, sortVal), and(eq(field, sortVal), gt(MongoFieldNames.ID, c.jobId))));
      } else {
        conditions.add(or(lt(field, sortVal), and(eq(field, sortVal), gt(MongoFieldNames.ID, c.jobId))));
      }
    } catch (IllegalArgumentException ignored) {
      // Malformed cursor — ignore
    }
  }

  private static Object parseSortValue(JobQueryCursor cursor) {
    return switch (cursor.sortField) {
      case CREATED_AT, SCHEDULED_TIME, UPDATED_AT -> Date.from(Instant.parse(cursor.sortValue));
      case PRIORITY -> Integer.parseInt(cursor.sortValue);
      case STATUS -> cursor.sortValue;
    };
  }

  // ── Sort builders ────────────────────────────────────────────────────────

  private static Bson buildSort(JobFilter filter) {
    if (filter == null) {
      return orderBy(descending(MongoFieldNames.CREATED_AT), ascending(MongoFieldNames.ID));
    }
    JobQuerySortField field =
        filter.sortField() != null ? filter.sortField() : JobQuerySortField.CREATED_AT;
    String col = sortField(field);
    Bson primary = filter.sortAscending() ? ascending(col) : descending(col);
    return orderBy(primary, ascending(MongoFieldNames.ID));
  }

  private static Bson buildArchiveSort(JobFilter filter) {
    if (filter == null) {
      return orderBy(descending(MongoFieldNames.ORIGINAL_CREATED_AT), ascending(MongoFieldNames.ID));
    }
    JobQuerySortField field =
        filter.sortField() != null ? filter.sortField() : JobQuerySortField.CREATED_AT;
    String col = archiveSortField(field);
    Bson primary = filter.sortAscending() ? ascending(col) : descending(col);
    return orderBy(primary, ascending(MongoFieldNames.ID));
  }

  private static String sortField(JobQuerySortField field) {
    return switch (field) {
      case CREATED_AT -> MongoFieldNames.CREATED_AT;
      case SCHEDULED_TIME -> MongoFieldNames.SCHEDULED_TIME;
      case UPDATED_AT -> MongoFieldNames.UPDATED_AT;
      case PRIORITY -> MongoFieldNames.PRIORITY;
      case STATUS -> MongoFieldNames.STATUS;
    };
  }

  private static String archiveSortField(JobQuerySortField field) {
    return switch (field) {
      case CREATED_AT -> MongoFieldNames.ORIGINAL_CREATED_AT;
      case SCHEDULED_TIME -> MongoFieldNames.SCHEDULED_TIME;
      case UPDATED_AT -> MongoFieldNames.ARCHIVED_AT;
      case PRIORITY -> MongoFieldNames.PRIORITY;
      case STATUS -> MongoFieldNames.FINAL_STATUS;
    };
  }

  private static Comparator<JobEntity> mergeComparator(JobFilter filter) {
    JobQuerySortField field = (filter == null || filter.sortField() == null)
        ? JobQuerySortField.CREATED_AT : filter.sortField();
    boolean asc = filter != null && filter.sortAscending();

    Comparator<JobEntity> cmp = switch (field) {
      case CREATED_AT -> Comparator.comparing(JobEntity::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder()));
      case SCHEDULED_TIME -> Comparator.comparing(JobEntity::getScheduledTime, Comparator.nullsLast(Comparator.naturalOrder()));
      case UPDATED_AT -> Comparator.comparing(JobEntity::getUpdatedAt, Comparator.nullsLast(Comparator.naturalOrder()));
      case PRIORITY -> Comparator.comparing(e -> e.getPriority() != null ? e.getPriority().ordinal() : 0);
      case STATUS -> Comparator.comparing(e -> e.getStatus() != null ? e.getStatus().name() : "");
    };

    if (!asc) {
      cmp = cmp.reversed();
    }
    // UUID tiebreaker: UUIDv7 is monotonically increasing so natural UUID comparison is correct
    Comparator<JobEntity> finalCmp = cmp;
    return finalCmp.thenComparing(JobEntity::getId, Comparator.nullsLast(Comparator.naturalOrder()));
  }

  // ── Helpers ─────────────────────────────────────────────────────────────

  private static boolean useArchive(JobFilter filter) {
    return filter != null
        && filter.includeArchived()
        && (filter.callerPrincipal() == null || filter.callerPrincipal().isEmpty());
  }
}
