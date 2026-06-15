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
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPTS;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.DEPENDS_ON;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_START_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.HEARTBEAT_TS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IDEMPOTENCY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_RETRIES;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.QUEUE_WAIT_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;

/**
 * Core CRUD + read-side stats for the {@code scheduler_job} collection. The longest op class
 * because it covers both the synchronous hot-path ({@link #save}, {@link #findById}) and the
 * read-only analytics surface that the scheduler exposes for dashboards.
 */
final class MongoJobCrudOperations {

  private static final Logger log = Logger.getLogger(MongoJobCrudOperations.class);
  private static final String STATUS_PENDING = JobStatus.PENDING.name();
  private static final String STATUS_RUNNING = JobStatus.RUNNING.name();
  private static final String STATUS_SUCCEEDED = JobStatus.SUCCEEDED.name();
  private static final String STATUS_FAILED = JobStatus.FAILED.name();
  private static final String TYPE_BATCH_CHILD = JobExecutionType.BATCH_CHILD.name();
  private static final String TYPE_RECURRING = JobExecutionType.RECURRING.name();

  private final MongoStoreContext ctx;

  MongoJobCrudOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
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
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
    Instant now = Instant.now();
    job.setCreatedAt(now);
    job.setUpdatedAt(now);
    if (job.getVersion() == null) {
      job.setVersion(0);
    }
    truncateInstantsToMillis(job);
    try {
      ctx.jobs().insertOne(DocumentMapper.toDocument(job));
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateIdempotencyKey(e)) {
        throw new DuplicateIdempotencyKeyException(job.getIdempotencyKey(), e);
      }
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
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
    UpdateResult result;
    try {
      result =
          ctx.jobs()
              .replaceOne(
                  and(eq(ID, job.getId()), eq(VERSION, expectedVersion)),
                  doc,
                  new ReplaceOptions().upsert(false));
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        job.setVersion(expectedVersion);
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
      }
      throw e;
    }
    if (result.getMatchedCount() == 0) {
      job.setVersion(expectedVersion);
      throw new RatchetOptimisticLockException(
          "Concurrent modification on job "
              + job.getId()
              + " (expectedVersion="
              + expectedVersion
              + ")");
    }
    return job;
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
    ctx.jobs().deleteOne(eq(ID, id));
  }

  JobStatus getJobStatus(UUID id) {
    Document doc = ctx.jobs().find(eq(ID, id)).projection(new Document(STATUS, 1)).first();
    if (doc == null) {
      return null;
    }
    return JobStatus.valueOf(doc.getString(STATUS));
  }

  List<JobEntity> findByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : ctx.jobs().find(in(ID, ids))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    Document doc =
        ctx.jobs()
            .find(and(eq(BUSINESS_KEY, businessKey), in(STATUS, MongoStoreContext.ACTIVE_STATUSES)))
            .limit(1)
            .first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    Document doc = ctx.jobs().find(eq(IDEMPOTENCY_KEY, idempotencyKey)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
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
        .countDocuments(and(eq(STATUS, STATUS_PENDING), eq(PRIORITY, priority.ordinal())));
  }

  Map<JobPriority, Long> countPendingJobsByPriorities() {
    List<Document> pipeline =
        List.of(
            new Document("$match", new Document(STATUS, STATUS_PENDING)),
            new Document(
                "$group",
                new Document(ID, "$" + PRIORITY).append("count", new Document("$sum", 1))));
    Map<JobPriority, Long> counts = new EnumMap<>(JobPriority.class);
    JobPriority[] values = JobPriority.values();
    for (Document doc : ctx.jobs().aggregate(pipeline)) {
      Object rawPriority = doc.get(ID);
      if (rawPriority instanceof Number priority) {
        int ordinal = priority.intValue();
        if (ordinal >= 0 && ordinal < values.length) {
          Number count = numberField(doc, "count");
          if (count != null) {
            counts.put(values[ordinal], count.longValue());
          }
        }
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
    try {
      ctx.jobs().insertMany(docs);
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use during bulk insert", e);
      }
      throw ctx.translateTransientStoreException("bulk insert jobs", e);
    }
  }

  int deleteJobsByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    DeleteResult result = ctx.jobs().deleteMany(in(ID, ids));
    return (int) result.getDeletedCount();
  }

  int deleteDlqOlderThan(Instant cutoff) {
    DeleteResult result =
        ctx.jobs()
            .deleteMany(
                and(
                    eq(STATUS, STATUS_FAILED),
                    new Document(
                        "$expr", new Document("$gte", List.of("$" + ATTEMPTS, "$" + MAX_RETRIES))),
                    or(
                        lt(TERMINATED_AT, DocumentMapper.toDate(cutoff)),
                        and(
                            exists(TERMINATED_AT, false),
                            lt(UPDATED_AT, DocumentMapper.toDate(cutoff))))));
    return (int) result.getDeletedCount();
  }

  int resetOrphanJobs(Duration grace) {
    // Use Duration directly — toMinutes() truncates sub-minute values.
    return resetOrphanJobsBefore(Instant.now().minus(grace));
  }

  int resetOrphanJobsBefore(Instant cutoffInstant) {
    Date cutoff = DocumentMapper.toDate(cutoffInstant);

    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            List<String> activeNodeIds = new ArrayList<>();
            for (Document doc : ctx.nodes().find(session, gte(HEARTBEAT_TS, cutoff))) {
              activeNodeIds.add(doc.getString(ID));
            }

            Bson filter;
            if (activeNodeIds.isEmpty()) {
              filter = and(eq(STATUS, STATUS_RUNNING), lt(PICKED_AT, cutoff));
            } else {
              filter =
                  and(
                      eq(STATUS, STATUS_RUNNING),
                      nin(PICKED_BY, activeNodeIds),
                      lt(PICKED_AT, cutoff));
            }

            UpdateResult result =
                ctx.jobs()
                    .updateMany(
                        session,
                        filter,
                        combine(
                            set(STATUS, STATUS_PENDING),
                            set(PICKED_BY, null),
                            set(PICKED_AT, null),
                            set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                            inc(VERSION, 1)));
            return (int) result.getModifiedCount();
          });
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs", e);
    }
  }

  int resetOrphanJobsForNode(String nodeId) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(eq(STATUS, STATUS_RUNNING), eq(PICKED_BY, nodeId)),
                combine(
                    set(STATUS, STATUS_PENDING),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }
}
