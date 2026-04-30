package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPTS;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.DEPENDS_ON;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_START_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.HEARTBEAT_TS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IDEMPOTENCY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_RETRIES;
import static run.ratchet.store.mongodb.MongoFieldNames.NEXT_FIRE;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.QUEUE_WAIT_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.id.UuidV7Factory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * Core CRUD + read-side stats for the {@code scheduler_job} collection. The longest op class
 * because it covers both the synchronous hot-path ({@link #save}, {@link #findById}) and the
 * read-only analytics surface that the scheduler exposes for dashboards.
 */
final class MongoJobCrudOperations {

  private static final Logger log = Logger.getLogger(MongoJobCrudOperations.class);

  private final MongoStoreContext ctx;

  MongoJobCrudOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  JobEntity save(JobEntity job) {
    Instant now = Instant.now();
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
      job.setCreatedAt(now);
      job.setUpdatedAt(now);
      if (job.getVersion() == null) {
        job.setVersion(0);
      }
      Document doc = DocumentMapper.toDocument(job);
      ctx.jobs().insertOne(doc);
      return job;
    }
    job.setUpdatedAt(now);

    // optimistic lock via version match — mismatch throws RatchetOptimisticLockException.
    // Version is bumped before replaceOne; rolled back on match failure so the caller's entity
    // reflects reality (prevents phantom version on reuse after catch).
    Integer expectedVersion = job.getVersion() != null ? job.getVersion() : 0;
    job.setVersion(expectedVersion + 1);
    Document doc = DocumentMapper.toDocument(job);
    UpdateResult result =
        ctx.jobs()
            .replaceOne(
                and(eq(ID, job.getId()), eq(VERSION, expectedVersion)),
                doc,
                new ReplaceOptions().upsert(false));
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

  List<JobEntity> findDependants(UUID parentJobId) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : ctx.jobs().find(eq(DEPENDS_ON, parentJobId))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  Optional<Instant> findEarliestRecurringNextFire() {
    Document doc =
        ctx.jobs()
            .find(and(eq(JOB_TYPE, "RECURRING"), eq(STATUS, "PENDING"), ne(NEXT_FIRE, null)))
            .sort(ascending(NEXT_FIRE))
            .projection(new Document(NEXT_FIRE, 1))
            .limit(1)
            .first();
    if (doc == null || doc.getDate(NEXT_FIRE) == null) {
      return Optional.empty();
    }
    return Optional.of(DocumentMapper.toInstant(doc.getDate(NEXT_FIRE)));
  }

  long countPendingJobs() {
    return ctx.jobs().countDocuments(eq(STATUS, "PENDING"));
  }

  long countJobsByStatus(JobStatus status) {
    return ctx.jobs().countDocuments(eq(STATUS, status.name()));
  }

  long countActiveJobs(JobExecutionType jobType) {
    return ctx.jobs()
        .countDocuments(
            and(eq(JOB_TYPE, jobType.name()), in(STATUS, List.of("PENDING", "RUNNING"))));
  }

  long countActiveNodes() {
    return ctx.nodes().countDocuments();
  }

  long countReadyJobs(Instant now) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "PENDING"), lte(SCHEDULED_TIME, DocumentMapper.toDate(now))));
  }

  long countStuckJobs(Instant stuckThreshold) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "RUNNING"), lt(PICKED_AT, DocumentMapper.toDate(stuckThreshold))));
  }

  long countLongRunningJobs(Instant threshold) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "RUNNING"), lt(EXECUTION_START_TIME, DocumentMapper.toDate(threshold))));
  }

  long countPendingBatchChildren() {
    return ctx.jobs().countDocuments(and(eq(JOB_TYPE, "BATCH_CHILD"), eq(STATUS, "PENDING")));
  }

  long countPendingJobsByPriority(JobPriority priority) {
    return ctx.jobs().countDocuments(and(eq(STATUS, "PENDING"), eq(PRIORITY, priority.ordinal())));
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    return ctx.jobs().countDocuments(and(eq(STATUS, "PENDING"), eq(JOB_TYPE, jobType.name())));
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
    if (result == null || result.getInteger("total", 0) == 0) {
      return 0.0;
    }
    return result.getInteger("retried", 0) / (double) result.getInteger("total");
  }

  double getAverageProcessingTime(Instant since) {
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document(STATUS, "SUCCEEDED")
                    .append(UPDATED_AT, new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document(ID, null)
                    .append("avg", new Document("$avg", "$" + EXECUTION_DURATION_MS))));
    Document result = ctx.jobs().aggregate(pipeline).first();
    if (result == null || result.get("avg") == null) {
      return 0.0;
    }
    return ((Number) result.get("avg")).doubleValue();
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
    Document result = ctx.batches().aggregate(pipeline).first();
    if (result == null || result.get("avg") == null) {
      return 0.0;
    }
    return ((Number) result.get("avg")).doubleValue();
  }

  Optional<Instant> getOldestPendingJobTime() {
    Document doc =
        ctx.jobs()
            .find(eq(STATUS, "PENDING"))
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
    // Prefer MongoDB 7.0+ $percentile; fall back to sort+skip for older servers.
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document(QUEUE_WAIT_MS, new Document("$ne", null)).append(STATUS, "SUCCEEDED")),
            new Document(
                "$group",
                new Document(ID, null)
                    .append(
                        "p",
                        new Document(
                            "$percentile",
                            new Document("input", "$" + QUEUE_WAIT_MS)
                                .append("p", List.of(percentile))
                                .append("method", "approximate")))));
    try {
      Document result = ctx.jobs().aggregate(pipeline).first();
      if (result != null && result.get("p") != null) {
        @SuppressWarnings("unchecked")
        List<Number> pValues = (List<Number>) result.get("p");
        if (!pValues.isEmpty()) {
          return pValues.get(0).longValue();
        }
      }
    } catch (Exception e) {
      log.debug("$percentile aggregation not available, using sort+skip approximation");
    }
    long total = ctx.jobs().countDocuments(and(ne(QUEUE_WAIT_MS, null), eq(STATUS, "SUCCEEDED")));
    if (total == 0) {
      return 0;
    }
    long skipCount = (long) (total * percentile);
    Document doc =
        ctx.jobs()
            .find(and(ne(QUEUE_WAIT_MS, null), eq(STATUS, "SUCCEEDED")))
            .sort(ascending(QUEUE_WAIT_MS))
            .skip((int) Math.min(skipCount, Integer.MAX_VALUE))
            .limit(1)
            .projection(new Document(QUEUE_WAIT_MS, 1))
            .first();
    return doc == null || doc.getLong(QUEUE_WAIT_MS) == null ? 0 : doc.getLong(QUEUE_WAIT_MS);
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
      docs.add(DocumentMapper.toDocument(job));
    }
    ctx.jobs().insertMany(docs);
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
                    eq(STATUS, "FAILED"),
                    new Document(
                        "$expr", new Document("$gte", List.of("$" + ATTEMPTS, "$" + MAX_RETRIES))),
                    lt(UPDATED_AT, DocumentMapper.toDate(cutoff))));
    return (int) result.getDeletedCount();
  }

  int resetOrphanJobs(Duration grace) {
    // Use Duration directly — toMinutes() truncates sub-minute values.
    Date cutoff = DocumentMapper.toDate(Instant.now().minus(grace));

    List<String> activeNodeIds = new ArrayList<>();
    for (Document doc : ctx.nodes().find(gte(HEARTBEAT_TS, cutoff))) {
      activeNodeIds.add(doc.getString(ID));
    }

    Bson filter;
    if (activeNodeIds.isEmpty()) {
      filter = and(eq(STATUS, "RUNNING"), lt(PICKED_AT, cutoff));
    } else {
      filter = and(eq(STATUS, "RUNNING"), nin(PICKED_BY, activeNodeIds), lt(PICKED_AT, cutoff));
    }

    UpdateResult result =
        ctx.jobs()
            .updateMany(
                filter,
                combine(
                    set(STATUS, "PENDING"),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  int resetOrphanJobsForNode(String nodeId) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(eq(STATUS, "RUNNING"), eq(PICKED_BY, nodeId)),
                combine(
                    set(STATUS, "PENDING"),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }
}
