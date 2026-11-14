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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;
import static run.ratchet.store.entity.JobExecutionEntity.STALE_EXECUTION_ERROR_MESSAGE;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPTS;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.CLAIM_SEQ;
import static run.ratchet.store.mongodb.MongoFieldNames.CRASH_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.DEPENDS_ON;
import static run.ratchet.store.mongodb.MongoFieldNames.ENDED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.ERROR_MESSAGE;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_START_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.HEARTBEAT_TS;
import static run.ratchet.store.mongodb.MongoFieldNames.HELD_UNTIL;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IDEMPOTENCY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.IS_PAUSED;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_RETRIES;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_TABLE;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.QUEUE_WAIT_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.RECURRING_MASTER_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STARTED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;
import static run.ratchet.store.mongodb.MongoStoreContext.TRANSACTION_OPTIONS;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.OrphanRecovery;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.StatusClassifier;

/**
 * Core CRUD + read-side stats for the {@code scheduler_job} collection. The longest op class
 * because it covers both the synchronous hot-path ({@link #save}, {@link #findById}) and the
 * read-only analytics surface that the scheduler exposes for dashboards.
 */
final class MongoJobCrudOperations {

  static final int ORPHAN_EXECUTION_CLOSE_BATCH = 500;

  private static final Logger log = Logger.getLogger(MongoJobCrudOperations.class);
  private static final String STATUS_PENDING = JobStatus.PENDING.name();
  private static final String STATUS_RUNNING = JobStatus.RUNNING.name();
  private static final String STATUS_SUCCEEDED = JobStatus.SUCCEEDED.name();
  private static final String STATUS_FAILED = JobStatus.FAILED.name();
  private static final String TYPE_BATCH_CHILD = JobExecutionType.BATCH_CHILD.name();
  private static final String TYPE_RECURRING = JobExecutionType.RECURRING.name();

  private final MongoStoreContext ctx;
  private final MongoBusinessKeyReservations reservations;

  MongoJobCrudOperations(MongoStoreContext ctx) {
    this(ctx, new MongoBusinessKeyReservations(ctx));
  }

  MongoJobCrudOperations(MongoStoreContext ctx, MongoBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  /**
   * Truncates the entity's Instant fields to millisecond precision before persisting. A BSON Date
   * stores milliseconds, so without this the in-memory entity keeps sub-millisecond nanos that the
   * document does not, and a later findById returns a different Instant than was written.
   */
  private static void truncateInstantsToMillis(JobEntity job) {
    job.setScheduledTime(DocumentMapper.truncateToMillis(job.getScheduledTime()));
    job.setPickedAt(DocumentMapper.truncateToMillis(job.getPickedAt()));
    job.setCreatedAt(DocumentMapper.truncateToMillis(job.getCreatedAt()));
    job.setUpdatedAt(DocumentMapper.truncateToMillis(job.getUpdatedAt()));
    job.setExecutionStartTime(DocumentMapper.truncateToMillis(job.getExecutionStartTime()));
    job.setExecutionEndTime(DocumentMapper.truncateToMillis(job.getExecutionEndTime()));
    job.setSignalTimeout(DocumentMapper.truncateToMillis(job.getSignalTimeout()));
    job.setSignalDeliveredAt(DocumentMapper.truncateToMillis(job.getSignalDeliveredAt()));
  }

  JobEntity create(JobEntity job) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(() -> create(session, job), TRANSACTION_OPTIONS);
    }
  }

  JobEntity create(ClientSession session, JobEntity job) {
    if (job.getId() == null) job.setId(UuidV7Factory.create());
    Instant now = Instant.now();
    job.setCreatedAt(now);
    job.setUpdatedAt(now);
    if (job.getVersion() == null) job.setVersion(0);
    truncateInstantsToMillis(job);
    try {
      reservations.syncForJob(session, job);
      MongoIdempotencyKeys.reserve(ctx.database(), session, List.of(job));
      ctx.jobs().insertOne(session, DocumentMapper.toDocument(job));
    } catch (RuntimeException e) {
      if (e instanceof BusinessKeyReservationConflictException) {
        throw e;
      }
      if (ctx.constraintDetector().isDuplicateIdempotencyKey(e)) {
        throw new DuplicateIdempotencyKeyException(job.getIdempotencyKey(), e);
      }
      throw e;
    }
    return job;
  }

  JobEntity save(JobEntity job) {
    Instant now = Instant.now();
    if (job.getId() == null) {
      return create(job);
    }
    job.setUpdatedAt(now);

    // optimistic lock via version match — mismatch throws RatchetOptimisticLockException.
    // Version is bumped before replaceOne; rolled back on match failure so the caller's entity
    // reflects reality (prevents phantom version on reuse after catch).
    Integer expectedVersion = job.getVersion() != null ? job.getVersion() : 0;
    job.setVersion(expectedVersion + 1);
    truncateInstantsToMillis(job);
    Document doc = DocumentMapper.toDocument(job);
    try {
      try (ClientSession session = ctx.startSession()) {
        session.withTransaction(
            () -> {
              MongoIdempotencyKeys.reserve(ctx.database(), session, List.of(job));
              reservations.syncForJob(session, job);
              // Whole-document saves must preserve the store-owned crash budget.
              Document current =
                  ctx.jobs()
                      .find(session, eq(ID, job.getId()))
                      .projection(
                          new Document(CRASH_COUNT, 1)
                              .append(CLAIM_SEQ, 1)
                              .append(SCHEDULED_TIME, 1)
                              .append(RESOURCE_NAME, 1))
                      .first();
              doc.put(CRASH_COUNT, current == null ? 0 : current.getInteger(CRASH_COUNT, 0));
              // Whole-document saves must preserve the store-owned claim sequence.
              doc.put(
                  CLAIM_SEQ,
                  current == null
                      ? 0L
                      : ((Number) current.getOrDefault(CLAIM_SEQ, 0L)).longValue());
              UpdateResult result =
                  ctx.jobs()
                      .replaceOne(
                          session,
                          and(eq(ID, job.getId()), eq(VERSION, expectedVersion)),
                          doc,
                          new ReplaceOptions().upsert(false));
              if (result.getMatchedCount() == 0) {
                throw new RatchetOptimisticLockException(
                    "Concurrent modification on job "
                        + job.getId()
                        + " (expectedVersion="
                        + expectedVersion
                        + ")");
              }
              if (current != null
                  && (!Objects.equals(current.getDate(SCHEDULED_TIME), doc.getDate(SCHEDULED_TIME))
                      || !Objects.equals(
                          current.getString(RESOURCE_NAME), doc.getString(RESOURCE_NAME)))) {
                ctx.resourceWaiters().deleteOne(session, eq(ID, job.getId()));
              }
              return true;
            },
            TRANSACTION_OPTIONS);
      }
    } catch (RuntimeException e) {
      job.setVersion(expectedVersion);
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
      }
      throw e;
    }
    return job;
  }

  Optional<UUID> findOriginalJobIdByIdempotencyKey(String key) {
    return MongoIdempotencyKeys.find(ctx.database(), key);
  }

  Optional<JobEntity> findById(UUID id) {
    Document doc = ctx.jobs().find(eq(ID, id)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  Optional<JobEntity> findByIdLatest(UUID id) {
    // MongoDB has no row-level locking; findOneAndUpdate is the atomic primitive.
    // Callers MUST mutate via a version-checked update path.
    return findById(id);
  }

  void delete(UUID id) {
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            reservations.releaseByOwner(session, id);
            ctx.jobs().deleteOne(session, eq(ID, id));
            return true;
          },
          TRANSACTION_OPTIONS);
    }
  }

  Optional<ClaimState> currentClaim(UUID jobId) {
    try {
      // Read the primary even when the client prefers secondaries: a lagging secondary could
      // still show a superseded claim as current. Read only majority-committed data, so a newer
      // claim that a failover later rolls back cannot mark this attempt superseded for good.
      Document doc =
          ctx.jobs()
              .withReadPreference(ReadPreference.primary())
              .withReadConcern(ReadConcern.MAJORITY)
              .find(eq(ID, jobId))
              .projection(new Document(STATUS, 1).append(CLAIM_SEQ, 1))
              .first();
      if (doc == null) {
        return Optional.empty();
      }
      JobStatus status = JobStatus.valueOf(doc.getString(STATUS));
      if (status == JobStatus.SUCCEEDED
          || status == JobStatus.FAILED
          || status == JobStatus.CANCELED) {
        return Optional.empty();
      }
      return Optional.of(
          new ClaimState(status, ((Number) doc.getOrDefault(CLAIM_SEQ, 0L)).longValue()));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("read current claim", e);
    }
  }

  JobStatus getJobStatus(UUID id) {
    // Read the primary even when the client prefers secondaries: callers act on this status
    // (a cancelled batch parent stops its children), so a lagging secondary must not answer.
    Document doc =
        ctx.jobs()
            .withReadPreference(ReadPreference.primary())
            .withReadConcern(ReadConcern.MAJORITY)
            .find(eq(ID, id))
            .projection(new Document(STATUS, 1))
            .first();
    if (doc == null) {
      return null;
    }
    return JobStatus.valueOf(doc.getString(STATUS));
  }

  Map<UUID, JobStatus> findStatusesByIds(Collection<UUID> ids) {
    List<UUID> uniqueIds = List.copyOf(new LinkedHashSet<>(ids));
    if (uniqueIds.size() > JobCrudStore.STATUS_READ_LIMIT) {
      throw new IllegalArgumentException("Too many job ids");
    }
    if (uniqueIds.isEmpty()) return Map.of();
    try {
      Map<UUID, JobStatus> result = new HashMap<>();
      for (Document doc :
          ctx.jobs()
              .withReadPreference(ReadPreference.primary())
              .withReadConcern(ReadConcern.MAJORITY)
              .find(in(ID, uniqueIds))
              .projection(new Document(ID, 1).append(STATUS, 1))) {
        String status = doc.getString(STATUS);
        if (status != null) result.put(doc.get(ID, UUID.class), JobStatus.valueOf(status));
      }
      return result;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job statuses by ids", e);
    }
  }

  List<JobEntity> findByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<JobEntity> results = new ArrayList<>();
    // Read the primary even when the client prefers secondaries: a batch cancel loads callback
    // branches here, and a lagging secondary could miss one written moments before.
    for (Document doc : ctx.jobs().withReadPreference(ReadPreference.primary()).find(in(ID, ids))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  Optional<BusinessKeyHolder> findBusinessKeyHolder(String businessKey) {
    Document reservation = ctx.businessKeyReservations().find(eq(ID, businessKey)).first();
    if (reservation == null) {
      return Optional.empty();
    }
    UUID id = reservation.get(OWNER_JOB_ID, UUID.class);
    String owner = reservation.getString(OWNER_TABLE);
    if (reservation.containsKey(HELD_UNTIL)) {
      Document job = ctx.jobs().find(eq(ID, id)).projection(new Document(JOB_TYPE, 1)).first();
      return job == null
          ? Optional.empty()
          : Optional.of(
              new BusinessKeyHolder(
                  id,
                  JobExecutionType.valueOf(job.getString(JOB_TYPE)).toPublicType(),
                  JobStatus.CANCELED));
    }
    if (BusinessKeyReservations.OWNER_TABLE_RECURRING.equals(owner)) {
      Document master =
          ctx.recurringJobs().find(eq(ID, id)).projection(new Document(IS_PAUSED, 1)).first();
      if (master == null) {
        return Optional.empty();
      }
      return Optional.of(
          new BusinessKeyHolder(
              id,
              JobType.RECURRING,
              Boolean.TRUE.equals(master.getBoolean(IS_PAUSED))
                  ? JobStatus.PAUSED
                  : JobStatus.PENDING));
    }
    if (BusinessKeyReservations.OWNER_TABLE_QUEUE.equals(owner)) {
      Document job =
          ctx.jobs()
              .find(and(eq(ID, id), in(STATUS, MongoStoreContext.ACTIVE_STATUSES)))
              .projection(new Document(STATUS, 1).append(JOB_TYPE, 1))
              .first();
      if (job == null) {
        return Optional.empty();
      }
      return Optional.of(
          new BusinessKeyHolder(
              id,
              JobExecutionType.valueOf(job.getString(JOB_TYPE)).toPublicType(),
              JobStatus.valueOf(job.getString(STATUS))));
    }
    return Optional.empty();
  }

  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    Document doc = ctx.jobs().find(eq(IDEMPOTENCY_KEY, idempotencyKey)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    List<JobEntity> results = new ArrayList<>();
    // Read the primary even when the client prefers secondaries: chain scheduling and cancel act
    // on these dependants, so a lagging secondary must not hide one written moments before.
    for (Document doc :
        ctx.jobs()
            .withReadPreference(ReadPreference.primary())
            .find(eq(DEPENDS_ON, parentJobId))
            .sort(ascending(CREATED_AT, ID))
            .skip(offset)
            .limit(limit)) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  long countPendingJobs() {
    return ctx.jobs().countDocuments(eq(STATUS, STATUS_PENDING));
  }

  long countJobsByStatus(JobStatus status) {
    return ctx.jobs().countDocuments(eq(STATUS, status.name()));
  }

  Map<JobStatus, Long> countJobsByStatuses() {
    List<Document> pipeline =
        List.of(
            new Document(
                "$group", new Document(ID, "$" + STATUS).append("count", new Document("$sum", 1))));
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Document doc : ctx.jobs().aggregate(pipeline)) {
      Object rawStatus = doc.get(ID);
      if (rawStatus instanceof String status) {
        Number count = numberField(doc, "count");
        if (count != null) {
          counts.put(JobStatus.valueOf(status), count.longValue());
        }
      }
    }
    return counts;
  }

  long countActiveJobs(JobExecutionType jobType) {
    return ctx.jobs()
        .countDocuments(
            and(eq(JOB_TYPE, jobType.name()), in(STATUS, List.of(STATUS_PENDING, STATUS_RUNNING))));
  }

  long countActiveNodes() {
    return ctx.nodes().countDocuments();
  }

  long countReadyJobs(Instant now) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, STATUS_PENDING), lte(SCHEDULED_TIME, DocumentMapper.toDate(now))));
  }

  long countStuckJobs(Instant stuckThreshold) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, STATUS_RUNNING), lt(PICKED_AT, DocumentMapper.toDate(stuckThreshold))));
  }

  long countLongRunningJobs(Instant threshold) {
    return ctx.jobs()
        .countDocuments(
            and(
                eq(STATUS, STATUS_RUNNING),
                lt(EXECUTION_START_TIME, DocumentMapper.toDate(threshold))));
  }

  long countPendingBatchChildren() {
    return ctx.jobs()
        .countDocuments(and(eq(JOB_TYPE, TYPE_BATCH_CHILD), eq(STATUS, STATUS_PENDING)));
  }

  long countPendingJobsByPriority(JobPriority priority) {
    return ctx.jobs()
        .countDocuments(and(eq(STATUS, STATUS_PENDING), eq(PRIORITY, priority.persistedCode())));
  }

  Map<JobPriority, Long> countPendingJobsByPriorities() {
    List<Document> pipeline =
        List.of(
            new Document("$match", new Document(STATUS, STATUS_PENDING)),
            new Document(
                "$group",
                new Document(ID, "$" + PRIORITY).append("count", new Document("$sum", 1))));
    Map<JobPriority, Long> counts = new EnumMap<>(JobPriority.class);
    for (Document doc : ctx.jobs().aggregate(pipeline)) {
      Object rawPriority = doc.get(ID);
      if (rawPriority instanceof Number priority) {
        JobPriority.findByPersistedCode(priority.intValue())
            .ifPresent(
                mappedPriority -> {
                  Number count = numberField(doc, "count");
                  if (count != null) {
                    counts.put(mappedPriority, count.longValue());
                  }
                });
      }
    }
    return counts;
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    return ctx.jobs().countDocuments(and(eq(STATUS, STATUS_PENDING), eq(JOB_TYPE, jobType.name())));
  }

  Map<JobExecutionType, Long> countPendingJobsByTypes() {
    List<Document> pipeline =
        List.of(
            new Document("$match", new Document(STATUS, STATUS_PENDING)),
            new Document(
                "$group",
                new Document(ID, "$" + JOB_TYPE).append("count", new Document("$sum", 1))));
    Map<JobExecutionType, Long> counts = new EnumMap<>(JobExecutionType.class);
    for (Document doc : ctx.jobs().aggregate(pipeline)) {
      Object rawType = doc.get(ID);
      if (rawType instanceof String type) {
        Number count = numberField(doc, "count");
        if (count != null) {
          counts.put(JobExecutionType.valueOf(type), count.longValue());
        }
      }
    }
    return counts;
  }

  long countJobsByStatusSince(JobStatus status, Instant since) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, status.name()), gte(UPDATED_AT, DocumentMapper.toDate(since))));
  }

  long countJobsWithRetries() {
    return ctx.jobs().countDocuments(new Document(ATTEMPTS, new Document("$gt", 0)));
  }

  double getRetryRateStats(Instant since) {
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document(UPDATED_AT, new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document(ID, null)
                    .append("total", new Document("$sum", 1))
                    .append(
                        "retried",
                        new Document(
                            "$sum",
                            new Document(
                                "$cond",
                                List.of(new Document("$gt", List.of("$" + ATTEMPTS, 0)), 1, 0))))));
    Document result = ctx.jobs().aggregate(pipeline).first();
    return ratioFromAggregate(result, "retried", "total");
  }

  double getAverageProcessingTime(Instant since) {
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document(STATUS, STATUS_SUCCEEDED)
                    .append(UPDATED_AT, new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document(ID, null)
                    .append("avg", new Document("$avg", "$" + EXECUTION_DURATION_MS))));
    return aggregateDouble(ctx.jobs(), pipeline, "avg");
  }

  double getAverageBatchSize(Instant since) {
    List<Document> pipeline =
        List.of(
            new Document(
                "$lookup",
                new Document("from", "scheduler_job")
                    .append("localField", ID)
                    .append("foreignField", ID)
                    .append("as", "job")),
            new Document("$unwind", "$job"),
            new Document(
                "$match",
                new Document("job.updated_at", new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document(ID, null).append("avg", new Document("$avg", "$" + TOTAL_ITEMS))));
    return aggregateDouble(ctx.batches(), pipeline, "avg");
  }

  Optional<Instant> getOldestPendingJobTime() {
    Document doc =
        ctx.jobs()
            .find(eq(STATUS, STATUS_PENDING))
            .sort(ascending(SCHEDULED_TIME))
            .projection(new Document(SCHEDULED_TIME, 1))
            .limit(1)
            .first();
    if (doc == null || doc.getDate(SCHEDULED_TIME) == null) {
      return Optional.empty();
    }
    return Optional.of(DocumentMapper.toInstant(doc.getDate(SCHEDULED_TIME)));
  }

  long getQueueWaitTimePercentile(double percentile) {
    if (Double.isNaN(percentile) || percentile < 0.0 || percentile > 1.0) {
      throw new IllegalArgumentException("percentile must be in [0.0, 1.0], got: " + percentile);
    }
    // Exact discrete nearest-rank over SUCCEEDED jobs, matching PostgreSQL PERCENTILE_DISC and the
    // MySQL CUME_DIST path. We deliberately avoid $percentile: its only mode is "approximate"
    // (t-digest), which would diverge from the SQL stores on the same data. Percentile reads are an
    // admin/metrics path, not the hot path, so the full ordering cost is acceptable.
    long total =
        ctx.jobs().countDocuments(and(ne(QUEUE_WAIT_MS, null), eq(STATUS, STATUS_SUCCEEDED)));
    if (total == 0) {
      return 0;
    }
    // Smallest 1-based rank k with k/total >= percentile, clamped to [1, total]; skip = k - 1.
    long rank = (long) Math.ceil(percentile * total);
    long skipCount = Math.max(1, Math.min(rank, total)) - 1;
    Document doc =
        ctx.jobs()
            .find(and(ne(QUEUE_WAIT_MS, null), eq(STATUS, STATUS_SUCCEEDED)))
            .sort(ascending(QUEUE_WAIT_MS))
            .skip((int) Math.min(skipCount, Integer.MAX_VALUE))
            .limit(1)
            .projection(new Document(QUEUE_WAIT_MS, 1))
            .first();
    return doc == null || doc.getLong(QUEUE_WAIT_MS) == null ? 0 : doc.getLong(QUEUE_WAIT_MS);
  }

  private static double aggregateDouble(
      MongoCollection<Document> collection, List<? extends Bson> pipeline, String field) {
    Number value = numberField(collection.aggregate(pipeline).first(), field);
    return value == null ? 0.0 : value.doubleValue();
  }

  private static double ratioFromAggregate(
      Document aggregateResult, String numeratorField, String denominatorField) {
    Number denominator = numberField(aggregateResult, denominatorField);
    if (denominator == null || denominator.doubleValue() == 0.0) {
      return 0.0;
    }
    Number numerator = numberField(aggregateResult, numeratorField);
    return numerator == null ? 0.0 : numerator.doubleValue() / denominator.doubleValue();
  }

  private static Number numberField(Document document, String field) {
    Object value = fieldValue(document, field);
    return value instanceof Number number ? number : null;
  }

  private static Object fieldValue(Document document, String field) {
    return document == null ? null : document.get(field);
  }

  void bulkInsert(List<JobEntity> jobList) {
    if (jobList.isEmpty()) {
      return;
    }
    List<Document> docs = prepareBulkDocuments(jobList);
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            insertPreparedDocuments(session, jobList, docs);
            return true;
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use during bulk insert", e);
      }
      throw ctx.translateTransientStoreException("bulk insert jobs", e);
    }
  }

  /** Joins the caller's Mongo transaction for composite store mutations. */
  void bulkInsert(ClientSession session, List<JobEntity> jobList) {
    if (jobList.isEmpty()) {
      return;
    }
    Objects.requireNonNull(session, "session");
    insertPreparedDocuments(session, jobList, prepareBulkDocuments(jobList));
  }

  private List<Document> prepareBulkDocuments(List<JobEntity> jobList) {
    Instant now = Instant.now();
    List<Document> docs = new ArrayList<>(jobList.size());
    for (JobEntity job : jobList) {
      if (job.getId() == null) {
        job.setId(UuidV7Factory.create());
      }
      if (job.getCreatedAt() == null) {
        job.setCreatedAt(now);
      }
      job.setUpdatedAt(now);
      if (job.getVersion() == null) {
        job.setVersion(0);
      }
      truncateInstantsToMillis(job);
      docs.add(DocumentMapper.toDocument(job));
    }
    return docs;
  }

  private void insertPreparedDocuments(
      ClientSession session, List<JobEntity> jobs, List<Document> docs) {
    MongoIdempotencyKeys.reserve(ctx.database(), session, jobs);
    for (JobEntity job : jobs) {
      reservations.syncForJob(session, job);
    }
    ctx.jobs().insertMany(session, docs);
  }

  int deleteJobsByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            reservations.releaseByOwners(session, ids);
            DeleteResult result = ctx.jobs().deleteMany(session, in(ID, ids));
            return (int) result.getDeletedCount();
          },
          TRANSACTION_OPTIONS);
    }
  }

  int deleteDlqOlderThan(Instant cutoff) {
    Bson filter =
        and(
            MongoPartitionOperations.unprotectedJobs(ctx),
            eq(STATUS, STATUS_FAILED),
            or(
                lt(TERMINATED_AT, DocumentMapper.toDate(cutoff)),
                and(exists(TERMINATED_AT, false), lt(UPDATED_AT, DocumentMapper.toDate(cutoff)))));
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            List<UUID> ids = new ArrayList<>();
            for (Document doc : ctx.jobs().find(session, filter).projection(new Document(ID, 1))) {
              UUID id = doc.get(ID, UUID.class);
              if (id != null) {
                ids.add(id);
              }
            }
            reservations.releaseByOwners(session, ids);
            DeleteResult result = ctx.jobs().deleteMany(session, filter);
            return (int) result.getDeletedCount();
          },
          TRANSACTION_OPTIONS);
    }
  }

  Optional<JobEntity> findOrphanCompletionSnapshot(UUID jobId) {
    Document doc =
        ctx.jobs()
            .find(eq(ID, jobId))
            .projection(
                new Document(JOB_TYPE, 1)
                    .append(DEPENDS_ON, 1)
                    .append(CLAIM_SEQ, 1)
                    .append(PICKED_BY, 1)
                    .append(ATTEMPTS, 1)
                    .append(MAX_RETRIES, 1)
                    .append(PRIORITY, 1)
                    .append(BUSINESS_KEY, 1)
                    .append(RECURRING_MASTER_ID, 1))
            .first();
    if (doc == null) {
      return Optional.empty();
    }
    JobEntity snapshot = new JobEntity();
    snapshot.setId(jobId);
    snapshot.setStatus(JobStatus.RUNNING);
    snapshot.setJobType(JobExecutionType.valueOf(doc.getString(JOB_TYPE)));
    snapshot.setDependsOn(doc.get(DEPENDS_ON, UUID.class));
    Number claim = doc.get(CLAIM_SEQ, Number.class);
    snapshot.setClaimSeq(claim == null ? 0L : claim.longValue());
    snapshot.setPickedBy(doc.getString(PICKED_BY));
    snapshot.setAttempts(doc.getInteger(ATTEMPTS, 0));
    snapshot.setMaxRetries(doc.getInteger(MAX_RETRIES, 0));
    snapshot.setPriority(RowValues.safeJobPriority(doc.getInteger(PRIORITY, 0)));
    snapshot.setBusinessKey(doc.getString(BUSINESS_KEY));
    snapshot.setRecurringMasterId(doc.get(RECURRING_MASTER_ID, UUID.class));
    return Optional.of(snapshot);
  }

  OrphanRecovery resetOrphanJobs(Duration grace, int maxCrashRedeliveries, int exhaustedLimit) {
    return resetOrphanJobsBefore(Instant.now().minus(grace), maxCrashRedeliveries, exhaustedLimit);
  }

  int closeStaleExecutions(Instant cutoffInstant, int limit) {
    if (limit <= 0) {
      return 0;
    }
    Date cutoff = DocumentMapper.toDate(cutoffInstant);
    Date now = new Date();
    int closed = 0;
    try (var cursor =
        ctx.executions()
            .find(and(eq(STATUS, STATUS_RUNNING), lt(STARTED_AT, cutoff)))
            .sort(ascending(STARTED_AT))
            .projection(new Document(ID, 1).append(JOB_ID, 1))
            .batchSize(ORPHAN_EXECUTION_CLOSE_BATCH)
            .iterator()) {
      while (closed < limit && cursor.hasNext()) {
        List<Document> batch = new ArrayList<>();
        List<UUID> jobIds = new ArrayList<>();
        while (batch.size() < ORPHAN_EXECUTION_CLOSE_BATCH && cursor.hasNext()) {
          Document row = cursor.next();
          batch.add(row);
          jobIds.add(row.get(JOB_ID, UUID.class));
        }
        Set<UUID> protectedIds = new HashSet<>();
        for (Document job :
            ctx.jobs()
                .find(
                    and(
                        in(ID, jobIds),
                        or(
                            eq(STATUS, STATUS_RUNNING),
                            gte(UPDATED_AT, cutoff),
                            gte(TERMINATED_AT, cutoff))))
                .projection(new Document(ID, 1))) {
          protectedIds.add(job.get(ID, UUID.class));
        }
        var candidates = batch.iterator();
        while (closed < limit && candidates.hasNext()) {
          List<UUID> closeIds = new ArrayList<>();
          while (closeIds.size() < limit - closed && candidates.hasNext()) {
            Document row = candidates.next();
            if (!protectedIds.contains(row.get(JOB_ID, UUID.class))) {
              closeIds.add(row.get(ID, UUID.class));
            }
          }
          if (!closeIds.isEmpty()) {
            closed +=
                (int)
                    ctx.executions()
                        .updateMany(
                            and(
                                in(ID, closeIds),
                                eq(STATUS, STATUS_RUNNING),
                                lt(STARTED_AT, cutoff)),
                            combine(
                                set(STATUS, STATUS_FAILED),
                                set(ENDED_AT, now),
                                set(ERROR_MESSAGE, STALE_EXECUTION_ERROR_MESSAGE)))
                        .getModifiedCount();
          }
        }
      }
      return closed;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("close stale executions", e);
    }
  }

  OrphanRecovery resetOrphanJobsBefore(
      Instant cutoffInstant, int maxCrashRedeliveries, int exhaustedLimit) {
    Date cutoff = DocumentMapper.toDate(cutoffInstant);
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            List<String> activeNodeIds = new ArrayList<>();
            for (Document doc : ctx.nodes().find(session, gte(HEARTBEAT_TS, cutoff))) {
              activeNodeIds.add(doc.getString(ID));
            }
            Bson orphan =
                and(
                    eq(STATUS, STATUS_RUNNING),
                    nin(PICKED_BY, activeNodeIds),
                    lt(PICKED_AT, cutoff));
            return recoverOrphans(session, orphan, maxCrashRedeliveries, exhaustedLimit, false);
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("recover orphan jobs", e);
    }
  }

  OrphanRecovery resetOrphanJobsForNode(
      String nodeId, int maxCrashRedeliveries, int exhaustedLimit) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () ->
              recoverOrphans(
                  session,
                  and(eq(STATUS, STATUS_RUNNING), eq(PICKED_BY, nodeId)),
                  maxCrashRedeliveries,
                  exhaustedLimit,
                  true),
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("recover orphan jobs for node", e);
    }
  }

  private OrphanRecovery recoverOrphans(
      ClientSession session,
      Bson orphan,
      int maxCrashRedeliveries,
      int exhaustedLimit,
      boolean releaseOwner) {
    List<UUID> orphanIds = new ArrayList<>();
    for (Document doc : ctx.jobs().find(session, orphan).projection(new Document(ID, 1))) {
      orphanIds.add(doc.get(ID, UUID.class));
    }
    if (!orphanIds.isEmpty()) {
      Date endedAt = DocumentMapper.toDate(Instant.now());
      for (int start = 0; start < orphanIds.size(); start += ORPHAN_EXECUTION_CLOSE_BATCH) {
        List<UUID> batch =
            orphanIds.subList(
                start, Math.min(start + ORPHAN_EXECUTION_CLOSE_BATCH, orphanIds.size()));
        ctx.executions()
            .updateMany(
                session,
                and(in(JOB_ID, batch), eq(STATUS, STATUS_RUNNING)),
                combine(
                    set(STATUS, STATUS_FAILED),
                    set(ENDED_AT, endedAt),
                    set(ERROR_MESSAGE, ORPHAN_RECOVERY_ERROR_MESSAGE)));
      }
    }
    // Missing means zero, including when the configured budget is zero.
    Bson atLimit =
        maxCrashRedeliveries == 0
            ? or(gte(CRASH_COUNT, 0), exists(CRASH_COUNT, false))
            : gte(CRASH_COUNT, maxCrashRedeliveries);
    Bson hasBudget =
        maxCrashRedeliveries == 0
            ? and(exists(CRASH_COUNT, true), not(gte(CRASH_COUNT, 0)))
            : not(gte(CRASH_COUNT, maxCrashRedeliveries));
    UpdateResult reset =
        ctx.jobs()
            .updateMany(
                session,
                and(orphan, hasBudget),
                combine(
                    set(STATUS, STATUS_PENDING),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    inc(CRASH_COUNT, 1),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    Bson exhaustedFilter = and(orphan, atLimit);
    List<ExhaustedOrphan> exhausted = new ArrayList<>();
    if (exhaustedLimit > 0) {
      for (Document doc :
          ctx.jobs()
              .find(session, exhaustedFilter)
              .projection(
                  new Document(ID, 1)
                      .append(CLAIM_SEQ, 1)
                      .append(CRASH_COUNT, 1)
                      .append(PICKED_BY, 1))
              .sort(ascending(ID))
              .limit(exhaustedLimit)) {
        Number claim = doc.get(CLAIM_SEQ, Number.class);
        exhausted.add(
            new ExhaustedOrphan(
                doc.get(ID, UUID.class),
                claim == null ? 0L : claim.longValue(),
                doc.getInteger(CRASH_COUNT, 0),
                doc.getString(PICKED_BY)));
      }
    }
    if (releaseOwner) {
      ctx.jobs().updateMany(session, exhaustedFilter, set(PICKED_BY, null));
    }
    return new OrphanRecovery((int) reset.getModifiedCount(), List.copyOf(exhausted));
  }

  private static boolean holdsBusinessKey(JobEntity job) {
    return job.getBusinessKey() != null
        && StatusClassifier.isLiveStatus(StatusClassifier.effectiveStatus(job.getStatus()));
  }

  int releaseBusinessKeyHolds(List<String> nodeIds) {
    return reservations.releaseBusinessKeyHolds(nodeIds);
  }

  int deleteExpiredBusinessKeyHolds(Instant now, int limit) {
    return reservations.deleteExpiredBusinessKeyHolds(now, limit);
  }
}
