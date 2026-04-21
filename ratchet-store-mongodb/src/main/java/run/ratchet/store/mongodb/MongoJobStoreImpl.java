package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static run.ratchet.store.mongodb.MongoFieldNames.*;

import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.TsidFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * MongoDB implementation of the {@link MongoJobStore} API.
 *
 * <p>Uses the MongoDB sync driver directly (no ODM). All state transitions use atomic {@code
 * findOneAndUpdate} operations. Tags are embedded in the job document as an array. IDs are
 * generated via {@link TsidFactory}.
 */
@ApplicationScoped
class MongoJobStoreImpl implements MongoJobStore {

  private static final Logger log = Logger.getLogger(MongoJobStoreImpl.class);

  private final MongoDatabase database;
  private final RatchetOptions options;
  private final MongoStoreContext ctx;

  private final ExecutorService claimExecutor =
      Executors.newFixedThreadPool(
          Math.max(2, Runtime.getRuntime().availableProcessors()),
          r -> {
            Thread t = new Thread(r, "ratchet-mongo-claim");
            t.setDaemon(true);
            return t;
          });

  @Inject
  MongoJobStoreImpl(MongoDatabase database, RatchetOptions options) {
    this.database = database;
    this.options = options;
    this.ctx = new MongoStoreContext(database, options.store().priorityBoostIntervalMinutes());
    options.node().explicitTsidNodeId().ifPresent(TsidFactory::configureNodeId);
  }

  @Override
  public JobEntity save(JobEntity job) {
    Instant now = Instant.now();
    if (job.getId() == null) {
      job.setId(TsidFactory.next());
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
      // Roll back the in-memory version bump so the caller's entity reflects reality.
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

  @Override
  public Optional<JobEntity> findById(long id) {
    Document doc = ctx.jobs().find(eq(ID, id)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  @Override
  public Optional<JobEntity> findByIdLatest(long id) {
    // MongoDB has no row-level locking; findOneAndUpdate is the atomic primitive.
    // Callers MUST mutate via a version-checked update path.
    return findById(id);
  }

  @Override
  public void delete(long id) {
    ctx.jobs().deleteOne(eq(ID, id));
  }

  @Override
  public JobStatus getJobStatus(long id) {
    Document doc = ctx.jobs().find(eq(ID, id)).projection(new Document(STATUS, 1)).first();
    if (doc == null) {
      return null;
    }
    return JobStatus.valueOf(doc.getString(STATUS));
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : ctx.jobs().find(in(ID, ids))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    Document doc =
        ctx.jobs()
            .find(and(eq(BUSINESS_KEY, businessKey), in(STATUS, MongoStoreContext.ACTIVE_STATUSES)))
            .limit(1)
            .first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    Document doc = ctx.jobs().find(eq(IDEMPOTENCY_KEY, idempotencyKey)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : ctx.jobs().find(eq(DEPENDS_ON, parentJobId))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
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

  @Override
  public long countPendingJobs() {
    return ctx.jobs().countDocuments(eq(STATUS, "PENDING"));
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return ctx.jobs().countDocuments(eq(STATUS, status.name()));
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return ctx.jobs()
        .countDocuments(
            and(eq(JOB_TYPE, jobType.name()), in(STATUS, List.of("PENDING", "RUNNING"))));
  }

  @Override
  public long countActiveNodes() {
    return ctx.nodes().countDocuments();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "PENDING"), lte(SCHEDULED_TIME, DocumentMapper.toDate(now))));
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "RUNNING"), lt(PICKED_AT, DocumentMapper.toDate(stuckThreshold))));
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, "RUNNING"), lt(EXECUTION_START_TIME, DocumentMapper.toDate(threshold))));
  }

  @Override
  public long countPendingBatchChildren() {
    return ctx.jobs().countDocuments(and(eq(JOB_TYPE, "BATCH_CHILD"), eq(STATUS, "PENDING")));
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return ctx.jobs().countDocuments(and(eq(STATUS, "PENDING"), eq(PRIORITY, priority.ordinal())));
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return ctx.jobs().countDocuments(and(eq(STATUS, "PENDING"), eq(JOB_TYPE, jobType.name())));
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return ctx.jobs()
        .countDocuments(
            and(eq(STATUS, status.name()), gte(UPDATED_AT, DocumentMapper.toDate(since))));
  }

  @Override
  public long countJobsWithRetries() {
    return ctx.jobs().countDocuments(new Document(ATTEMPTS, new Document("$gt", 0)));
  }

  @Override
  public double getRetryRateStats(Instant since) {
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

  @Override
  public double getAverageProcessingTime(Instant since) {
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

  @Override
  public double getAverageBatchSize(Instant since) {
    // Look up batch entities joined with their parent job updated_at
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

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
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

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    // Use $setWindowFields with $percentile (MongoDB 7.0+), fallback to sort+skip approximation
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
      // $percentile not supported — fall back to sort+skip
      log.debug("$percentile aggregation not available, using sort+skip approximation");
    }
    // Fallback: sort by queue_wait_ms, skip to percentile position
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

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(
            MongoStoreContext.EXECUTABLE_JOB_TYPES, SCHEDULED_TIME, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !MongoStoreContext.isPollerExecutable(jobType)) {
      return List.of();
    }
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of(jobType.name()), SCHEDULED_TIME, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobClaimDto);
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of("RECURRING"), NEXT_FIRE, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    ctx.jobs()
        .updateOne(
            eq(ID, id),
            combine(
                set(STATUS, status.name()),
                set(LAST_ERROR, errorMessage),
                set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                inc(VERSION, 1)));
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, expected.name())),
                  combine(
                      set(STATUS, newStatus.name()),
                      set(LAST_ERROR, error),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("compare-and-swap status", e);
    }
  }

  @Override
  public int incrementRetryAttempt(long id) {
    Document doc =
        ctx.jobs()
            .findOneAndUpdate(
                and(eq(ID, id), eq(STATUS, "RUNNING")),
                combine(
                    inc(ATTEMPTS, 1),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      return -1;
    }
    return doc.getInteger(ATTEMPTS);
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    Instant now = Instant.now();
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "PENDING")),
                combine(
                    set(STATUS, "RUNNING"),
                    set(PICKED_BY, nodeId),
                    set(PICKED_AT, DocumentMapper.toDate(now)),
                    set(UPDATED_AT, DocumentMapper.toDate(now)),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, "RUNNING")),
                  combine(
                      set(STATUS, "SUCCEEDED"),
                      set(JOB_RESULT, resultJson),
                      set(RESULT_TYPE, resultType),
                      set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                      set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                      set(EXECUTION_DURATION_MS, durationMs),
                      set(QUEUE_WAIT_MS, queueWaitMs),
                      set(LAST_ERROR, null),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded", e);
    }
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, "RUNNING")),
                  combine(
                      set(STATUS, "SUCCEEDED"),
                      set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                      set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                      set(EXECUTION_DURATION_MS, durationMs),
                      set(QUEUE_WAIT_MS, queueWaitMs),
                      set(LAST_ERROR, null),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded minimally", e);
    }
  }

  @Override
  public boolean markJobSucceededAndUpdateBatch(
      long jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      long batchId) {
    boolean jobUpdated =
        markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
    if (jobUpdated) {
      incrementCompletedAtomic(batchId);
    }
    return jobUpdated;
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), in(STATUS, List.of("RUNNING", "FAILED"))),
                combine(
                    set(STATUS, "PENDING"),
                    set(SCHEDULED_TIME, DocumentMapper.toDate(newScheduledTime)),
                    set(ATTEMPTS, attempts),
                    set(LAST_ERROR, error),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean pauseRecurring(long id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(JOB_TYPE, "RECURRING"), eq(STATUS, "PENDING")),
                combine(
                    set(STATUS, "PAUSED"),
                    set(PAUSED_FROM_STATUS, "PENDING"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean resumeRecurring(long id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(JOB_TYPE, "RECURRING"), eq(STATUS, "PAUSED")),
                combine(
                    set(STATUS, "PENDING"),
                    set(PAUSED_FROM_STATUS, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "RUNNING")),
                combine(
                    set(STATUS, "FAILED"),
                    set(LAST_ERROR, terminalError),
                    set(ATTEMPTS, totalAttempts),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean cancelJob(long id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), in(STATUS, List.of("PENDING", "RUNNING", "PAUSED"))),
                combine(
                    set(STATUS, "CANCELED"),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "RUNNING"), eq(PICKED_BY, nodeId)),
                combine(
                    set(STATUS, "PENDING"),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
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

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(TAGS, tag),
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(BUSINESS_KEY, businessKey),
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES),
                    lt(CREATED_AT, DocumentMapper.toDate(nodeStartTime)),
                    ne(BUSINESS_KEY, null),
                    nin(BUSINESS_KEY, registeredIds)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public boolean resetFailedToPending(long id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "FAILED")),
                combine(
                    set(STATUS, "PENDING"),
                    set(ATTEMPTS, 0),
                    set(LAST_ERROR, null),
                    set(SCHEDULED_TIME, DocumentMapper.toDate(Instant.now())),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, expected.name())),
                combine(
                    set(STATUS, "PAUSED"),
                    set(PAUSED_FROM_STATUS, expected.name()),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "PAUSED")),
                combine(
                    set(STATUS, target.name()),
                    set(PAUSED_FROM_STATUS, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    Document before =
        ctx.jobs()
            .findOneAndUpdate(
                and(eq(ID, id), eq(STATUS, "PAUSED")),
                List.of(
                    new Document(
                        "$set",
                        new Document()
                            .append(
                                STATUS,
                                new Document(
                                    "$ifNull", List.of("$" + PAUSED_FROM_STATUS, "PENDING")))
                            .append(PAUSED_FROM_STATUS, null)
                            .append(UPDATED_AT, new Date())
                            .append(VERSION, new Document("$add", List.of("$" + VERSION, 1))))),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return null;
    }
    String pausedFrom = before.getString(PAUSED_FROM_STATUS);
    return pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
  }

  @Override
  public void bulkInsert(List<JobEntity> jobList) {
    if (jobList.isEmpty()) {
      return;
    }
    Instant now = Instant.now();
    List<Document> docs = new ArrayList<>(jobList.size());
    for (JobEntity job : jobList) {
      if (job.getId() == null) {
        job.setId(TsidFactory.next());
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

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    DeleteResult result = ctx.jobs().deleteMany(in(ID, ids));
    return (int) result.getDeletedCount();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
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

  @Override
  public int resetOrphanJobs(Duration grace) {
    // Use Duration directly — toMinutes() truncates sub-minute values
    Date cutoff = DocumentMapper.toDate(Instant.now().minus(grace));

    // Find active node IDs
    List<String> activeNodeIds = new ArrayList<>();
    for (Document doc : ctx.nodes().find(gte(HEARTBEAT_TS, cutoff))) {
      activeNodeIds.add(doc.getString(ID));
    }

    Bson filter;
    if (activeNodeIds.isEmpty()) {
      // All nodes are inactive — reset all running jobs past grace
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

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
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

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    Document doc = DocumentMapper.toDocument(batch);
    ctx.batches().replaceOne(eq(ID, batch.getId()), doc, new ReplaceOptions().upsert(true));
    return batch;
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    Document doc = ctx.batches().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchEntity(doc));
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    List<BatchEntity> result = new ArrayList<>();
    for (Document doc : ctx.batches().find(in(ID, batchIds))) {
      result.add(DocumentMapper.toBatchEntity(doc));
    }
    return result;
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    Document doc =
        ctx.batches()
            .findOneAndUpdate(
                eq(ID, batchId),
                inc(COMPLETED_ITEMS, 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    Document doc =
        ctx.batches()
            .findOneAndUpdate(
                eq(ID, batchId),
                inc(FAILED_ITEMS, 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    UpdateResult result =
        ctx.batches()
            .updateOne(
                and(
                    eq(ID, batchId),
                    eq(COMPLETION_PROCESSED, false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document(
                                    "$add", List.of("$" + COMPLETED_ITEMS, "$" + FAILED_ITEMS)),
                                "$" + TOTAL_ITEMS)))),
                set(COMPLETION_PROCESSED, true));
    return result.getModifiedCount() > 0;
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    List<Long> ids = new ArrayList<>();
    FindIterable<Document> results =
        ctx.batches()
            .find(
                and(
                    eq(COMPLETION_PROCESSED, false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document(
                                    "$add", List.of("$" + COMPLETED_ITEMS, "$" + FAILED_ITEMS)),
                                "$" + TOTAL_ITEMS)))))
            .projection(new Document(ID, 1))
            .limit(limit);
    for (Document doc : results) {
      ids.add(doc.getLong(ID));
    }
    return ids;
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    UpdateResult result = ctx.batches().updateOne(eq(ID, batchId), set(TOTAL_ITEMS, totalItems));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    Date now = DocumentMapper.toDate(Instant.now());
    Date expiresAt = DocumentMapper.toDate(Instant.now().plus(ttl));

    try {
      // Attempt to upsert: insert if no lock exists, or update if lock is expired
      Document result =
          ctx.locks()
              .findOneAndUpdate(
                  and(eq(ID, name), lt(EXPIRES_AT, now)),
                  combine(
                      set(OWNER_NODE, nodeId),
                      set(LOCKED_AT, now),
                      set(EXPIRES_AT, expiresAt),
                      setOnInsert(ID, name)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

      // If we got a result with our nodeId, the lock was acquired (insert or expired-update)
      return result != null && nodeId.equals(result.getString(OWNER_NODE));
    } catch (MongoCommandException e) {
      // 11000 = duplicate key (lock already held)
      if (e.getErrorCode() == 11000) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void unlock(String name, String nodeId) {
    ctx.locks().deleteOne(and(eq(ID, name), eq(OWNER_NODE, nodeId)));
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    Date newExpiry = DocumentMapper.toDate(Instant.now().plus(extension));
    UpdateResult result =
        ctx.locks()
            .updateOne(and(eq(ID, name), eq(OWNER_NODE, nodeId)), set(EXPIRES_AT, newExpiry));
    return result.getModifiedCount() > 0;
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    Date tsDate = DocumentMapper.toDate(ts);
    ctx.nodes()
        .updateOne(
            eq(ID, nodeId),
            combine(set(HEARTBEAT_TS, tsDate), setOnInsert(STARTED_AT, tsDate)),
            new UpdateOptions().upsert(true));
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    Document doc = ctx.nodes().find(eq(ID, nodeId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toNodeEntity(doc));
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    List<NodeEntity> results = new ArrayList<>();
    for (Document doc : ctx.nodes().find(lt(HEARTBEAT_TS, DocumentMapper.toDate(cutoff)))) {
      results.add(DocumentMapper.toNodeEntity(doc));
    }
    return results;
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    DeleteResult result = ctx.nodes().deleteMany(lt(HEARTBEAT_TS, DocumentMapper.toDate(cutoff)));
    return (int) result.getDeletedCount();
  }

  @Override
  public Instant getDatabaseTime() {
    Document result = database.runCommand(new Document("serverStatus", 1).append("localTime", 1));
    Date localTime = result.getDate("localTime");
    return localTime != null ? localTime.toInstant() : Instant.now();
  }

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
    archive.setId(TsidFactory.next());
    Document doc = DocumentMapper.toDocument(archive);
    ctx.archives().insertOne(doc);
    return archive;
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobList, String reason, String archivedBy) {
    if (jobList.isEmpty()) {
      return 0;
    }
    List<Document> docs = new ArrayList<>(jobList.size());
    for (JobEntity job : jobList) {
      ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
      archive.setId(TsidFactory.next());
      docs.add(DocumentMapper.toDocument(archive));
    }
    ctx.archives().insertMany(docs);
    return docs.size();
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(
                and(
                    in(STATUS, MongoStoreContext.TERMINAL_STATUSES),
                    lt(UPDATED_AT, DocumentMapper.toDate(olderThan))))
            .sort(ascending(UPDATED_AT))
            .limit(limit)) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return ctx.jobs()
        .countDocuments(
            and(
                in(STATUS, MongoStoreContext.TERMINAL_STATUSES),
                lt(UPDATED_AT, DocumentMapper.toDate(olderThan))));
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    List<Bson> filters = new ArrayList<>();
    if (targetClass != null) {
      filters.add(eq(TARGET_CLASS, targetClass));
    }
    if (businessKey != null) {
      filters.add(eq(BUSINESS_KEY, businessKey));
    }
    if (from != null) {
      filters.add(gte(ARCHIVED_AT, DocumentMapper.toDate(from)));
    }
    if (to != null) {
      filters.add(lte(ARCHIVED_AT, DocumentMapper.toDate(to)));
    }

    Bson filter = filters.isEmpty() ? new Document() : and(filters);
    List<ArchivedJobEntity> results = new ArrayList<>();
    for (Document doc : ctx.archives().find(filter).sort(descending(ARCHIVED_AT)).limit(limit)) {
      results.add(DocumentMapper.toArchivedJobEntity(doc));
    }
    return results;
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    DeleteResult result =
        ctx.archives().deleteMany(lt(ARCHIVED_AT, DocumentMapper.toDate(olderThan)));
    return (int) result.getDeletedCount();
  }

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      execution.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(execution);
    ctx.executions().replaceOne(eq(ID, execution.getId()), doc, new ReplaceOptions().upsert(true));
    return execution;
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    List<JobExecutionEntity> results = new ArrayList<>();
    for (Document doc : ctx.executions().find(eq(JOB_ID, jobId)).sort(ascending(ATTEMPT))) {
      results.add(DocumentMapper.toJobExecutionEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    Document doc =
        ctx.executions().find(eq(JOB_ID, jobId)).sort(descending(ATTEMPT)).limit(1).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobExecutionEntity(doc));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return (int) ctx.executions().countDocuments(eq(JOB_ID, jobId));
  }

  @Override
  public void appendLog(JobLogEntity logEntry) {
    if (logEntry.getId() == null) {
      logEntry.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(logEntry);
    ctx.jobLogs().insertOne(doc);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    DeleteResult result = ctx.jobLogs().deleteMany(lt(TS, DocumentMapper.toDate(cutoff)));
    return (int) result.getDeletedCount();
  }

  @Override
  public void insertTags(long jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    // Tags are embedded in job document — use $addToSet with $each
    ctx.jobs()
        .updateOne(
            eq(ID, jobId),
            new Document("$addToSet", new Document(TAGS, new Document("$each", tags))));
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    Document before =
        ctx.jobs()
            .findOneAndUpdate(
                eq(ID, jobId),
                set(TAGS, List.of()),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return 0;
    }
    List<String> oldTags = before.getList(TAGS, String.class);
    return oldTags == null ? 0 : oldTags.size();
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    List<Long> ids = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(eq(TAGS, tag))
            .projection(new Document(ID, 1))
            .sort(ascending(ID))
            .skip(offset)
            .limit(limit)) {
      ids.add(doc.getLong(ID));
    }
    return ids;
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Document doc :
        ctx.jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document(TAGS, tag)),
                    new Document(
                        "$group",
                        new Document(ID, "$" + STATUS).append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document(ID, 1))))) {
      String status = doc.getString(ID);
      if (status != null) {
        counts.put(JobStatus.valueOf(status), ((Number) doc.get("count")).longValue());
      }
    }
    return counts;
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return aggregateStringCountsByTag(
        tag, new Document("$getField", new Document("field", paramKey).append("input", "$params")));
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return aggregateStringCountsByTag(tag, "$" + PICKED_BY);
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null) {
      condition.setId(TsidFactory.next());
      if (condition.getCreatedAt() == null) {
        condition.setCreatedAt(Instant.now());
      }
    }
    Document doc = DocumentMapper.toDocument(condition);
    ctx.workflowConditions()
        .replaceOne(eq(ID, condition.getId()), doc, new ReplaceOptions().upsert(true));
    return condition;
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    Document doc = ctx.workflowConditions().find(eq(ID, id)).first();
    return doc == null ? null : DocumentMapper.toWorkflowConditionEntity(doc);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.workflowConditions()
            .find(eq(PARENT_JOB_ID, parentJobId))
            .sort(ascending(CONDITION_PRIORITY))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc : ctx.workflowConditions().find(eq(CHILD_JOB_ID, childJobId))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.workflowConditions()
            .find(and(eq(PARENT_JOB_ID, parentJobId), eq(CONDITION_TYPE, type.name())))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public void deleteConditionById(long id) {
    ctx.workflowConditions().deleteOne(eq(ID, id));
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    ctx.workflowConditions().deleteMany(eq(PARENT_JOB_ID, parentJobId));
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    ctx.workflowConditions().deleteMany(eq(CHILD_JOB_ID, childJobId));
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return ctx.workflowConditions().countDocuments(eq(PARENT_JOB_ID, parentJobId));
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    Document doc = DocumentMapper.toDocument(metrics);
    ctx.batchMetrics()
        .replaceOne(eq(ID, metrics.getBatchId()), doc, new ReplaceOptions().upsert(true));
    return metrics;
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchMetricsEntity(doc));
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    ctx.batchMetrics()
        .updateOne(
            eq(ID, batchId), combine(inc(CHILD_EXECUTION_MS, durationMs), inc(SUCCESS_COUNT, 1)));
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    if (doc == null) {
      return;
    }
    Instant now = Instant.now();
    Date startedAt = doc.getDate(STARTED_AT);
    Long childExecutionMs = doc.getLong(CHILD_EXECUTION_MS);

    Long totalDurationMs = null;
    Long overheadMs = null;
    if (startedAt != null) {
      totalDurationMs = Duration.between(startedAt.toInstant(), now).toMillis();
      if (childExecutionMs != null) {
        overheadMs = totalDurationMs - childExecutionMs;
      }
    }

    ctx.batchMetrics()
        .updateOne(
            eq(ID, batchId),
            combine(
                set(COMPLETED_AT, DocumentMapper.toDate(now)),
                set(TOTAL_DURATION_MS, totalDurationMs),
                set(OVERHEAD_MS, overheadMs)));
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    ctx.batchMetrics().updateOne(eq(ID, batchId), set(CHILD_COUNT, childCount));
  }

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    if (alert.getId() == null) {
      alert.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(alert);
    ctx.dlqAlerts().replaceOne(eq(ID, alert.getId()), doc, new ReplaceOptions().upsert(true));
    return alert;
  }

  @Override
  public boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff) {
    return ctx.dlqAlerts()
            .countDocuments(
                and(
                    eq(JOB_ID, jobId),
                    eq(ERROR_HASH, errorHash),
                    gte(ALERT_SENT_AT, DocumentMapper.toDate(cutoff))))
        > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    // Atomically increment active_count only if it is below max_concurrent.
    // Uses $expr to compare two fields in the same document, ensuring no TOCTOU race.
    Document result =
        ctx.resourceLimits()
            .findOneAndUpdate(
                and(
                    eq(ID, resource),
                    expr(
                        new Document(
                            "$lt",
                            List.of(
                                new Document("$ifNull", List.of("$" + ACTIVE_COUNT, 0)),
                                "$" + MAX_CONCURRENT)))),
                inc(ACTIVE_COUNT, 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

    if (result == null) {
      return false;
    }

    ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
    permit.setId(TsidFactory.next());
    ctx.resourcePermits().insertOne(DocumentMapper.toDocument(permit));
    return true;
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    DeleteResult dr =
        ctx.resourcePermits().deleteOne(and(eq(RESOURCE_NAME, resource), eq(JOB_ID, jobId)));
    if (dr.getDeletedCount() > 0) {
      ctx.resourceLimits().updateOne(eq(ID, resource), inc(ACTIVE_COUNT, -1));
    }
  }

  @Override
  public void releaseAllPermits(long jobId) {
    List<String> resources = new ArrayList<>();
    ctx.resourcePermits()
        .find(eq(JOB_ID, jobId))
        .forEach(doc -> resources.add(doc.getString(RESOURCE_NAME)));
    DeleteResult dr = ctx.resourcePermits().deleteMany(eq(JOB_ID, jobId));
    if (dr.getDeletedCount() > 0) {
      for (String resource : resources) {
        ctx.resourceLimits().updateOne(eq(ID, resource), inc(ACTIVE_COUNT, -1));
      }
    }
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    Document doc = ctx.resourceLimits().find(eq(ID, resource)).first();
    if (doc == null) {
      return 5000;
    }
    return doc.getInteger(RETRY_DELAY_MS, 5000);
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    Instant now = Instant.now();
    ctx.resourceLimits()
        .updateOne(
            eq(ID, name),
            combine(
                set(MAX_CONCURRENT, maxConcurrent),
                set(RETRY_DELAY_MS, retryDelayMs),
                set(DESCRIPTION, description),
                set(UPDATED_AT, DocumentMapper.toDate(now)),
                setOnInsert(CREATED_AT, DocumentMapper.toDate(now)),
                setOnInsert(ACTIVE_COUNT, 0)),
            new UpdateOptions().upsert(true));
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    List<Document> orphanedPermits = new ArrayList<>();
    ctx.resourcePermits().find(in(NODE_ID, staleNodeIds)).forEach(orphanedPermits::add);
    DeleteResult result = ctx.resourcePermits().deleteMany(in(NODE_ID, staleNodeIds));
    orphanedPermits.stream()
        .map(doc -> doc.getString(RESOURCE_NAME))
        .distinct()
        .forEach(
            resource -> {
              long count =
                  orphanedPermits.stream()
                      .filter(doc -> resource.equals(doc.getString(RESOURCE_NAME)))
                      .count();
              ctx.resourceLimits().updateOne(eq(ID, resource), inc(ACTIVE_COUNT, (int) -count));
            });
    return (int) result.getDeletedCount();
  }

  @PreDestroy
  void shutdown() {
    claimExecutor.shutdown();
    try {
      if (!claimExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        claimExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      claimExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @PostConstruct
  void initializeCollections() {
    new MongoCollectionInitializer(database).initialize();
  }

  /** Finds candidate job IDs sorted by effective priority (raw priority + age-based boost). */
  private List<Long> findCandidatesByBoostedPriority(
      List<String> jobTypes, String timeColumn, int limit) {
    if (limit <= 0 || jobTypes.isEmpty()) {
      return List.of();
    }

    Date now = DocumentMapper.toDate(Instant.now());
    Bson match =
        new Document(
            "$match",
            new Document(STATUS, "PENDING")
                .append(JOB_TYPE, new Document("$in", jobTypes))
                .append(timeColumn, new Document("$lte", now)));
    Bson project =
        new Document(
            "$project",
            new Document(ID, 1)
                .append(timeColumn, 1)
                .append("effective_priority", effectivePriorityExpression(timeColumn, now)));
    Bson sort =
        new Document(
            "$sort", new Document("effective_priority", -1).append(timeColumn, 1).append(ID, 1));
    Bson batchLimit = new Document("$limit", limit);

    var query = ctx.jobs().aggregate(List.of(match, project, sort, batchLimit)).allowDiskUse(true);

    if (NEXT_FIRE.equals(timeColumn)) {
      query.hintString(MongoIndexHints.JOB_CLAIM_RECURRING);
    } else {
      query.hintString(MongoIndexHints.JOB_CLAIM_EXEC);
    }

    List<Long> ids = new ArrayList<>(limit);
    for (Document doc : query) {
      ids.add(doc.getLong(ID));
    }
    return ids;
  }

  private Object effectivePriorityExpression(String timeColumn, Date now) {
    Object priorityExpression =
        new Document("$ifNull", List.of("$" + PRIORITY, JobPriority.NORMAL.ordinal()));
    int priorityBoostInterval = options.store().priorityBoostIntervalMinutes();
    if (priorityBoostInterval <= 0) {
      return priorityExpression;
    }
    Object ageMillisExpression =
        new Document(
            "$max", List.of(0L, new Document("$subtract", List.of(now, "$" + timeColumn))));
    Object ageMinutesExpression =
        new Document("$floor", new Document("$divide", List.of(ageMillisExpression, 60_000L)));
    Object boostExpression =
        new Document(
            "$floor",
            new Document("$divide", List.of(ageMinutesExpression, priorityBoostInterval)));
    return new Document("$add", List.of(priorityExpression, boostExpression));
  }

  private <T> List<T> claimByIds(List<Long> ids, String nodeId, Function<Document, T> mapper) {
    Date nowDate = DocumentMapper.toDate(Instant.now());
    FindOneAndUpdateOptions opts =
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);

    List<CompletableFuture<Document>> futures =
        ids.stream()
            .map(
                id ->
                    CompletableFuture.supplyAsync(
                        () ->
                            ctx.jobs()
                                .findOneAndUpdate(
                                    and(eq(ID, id), eq(STATUS, "PENDING")),
                                    combine(
                                        set(STATUS, "RUNNING"),
                                        set(PICKED_BY, nodeId),
                                        set(PICKED_AT, nowDate),
                                        set(UPDATED_AT, nowDate),
                                        inc(VERSION, 1)),
                                    opts),
                        claimExecutor))
            .toList();

    List<T> claimed = new ArrayList<>();
    for (var future : futures) {
      try {
        Document doc = future.join();
        if (doc != null) {
          claimed.add(mapper.apply(doc));
        }
      } catch (CompletionException e) {
        log.warnf("Claim error: %s", e.getCause().getMessage());
      }
    }
    return claimed;
  }

  private Map<String, Long> aggregateStringCountsByTag(String tag, Object groupExpression) {
    Map<String, Long> counts = new TreeMap<>();
    for (Document doc :
        ctx.jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document(TAGS, tag)),
                    new Document(
                        "$group",
                        new Document(ID, groupExpression)
                            .append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document(ID, 1))))) {
      Object keyValue = doc.get(ID);
      if (!(keyValue instanceof String key) || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) doc.get("count")).longValue());
    }
    return counts;
  }

  private ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setOriginalJobId(job.getId());
    a.setFinalStatus(job.getStatus());
    a.setJobType(job.getJobType());
    a.setPriority(job.getPriority());
    a.setTotalAttempts(job.getAttempts());
    a.setMaxRetries(job.getMaxRetries());
    a.setBackoffPolicy(job.getBackoffPolicy());
    a.setBackoffParamMs(job.getBackoffParamMs());
    a.setTimeoutSec(job.getTimeoutSec());
    a.setTargetClass(job.getTargetClass());
    a.setMethodName(job.getMethodName());
    a.setBusinessKey(job.getBusinessKey());
    a.setCronExpr(job.getCronExpr());
    a.setZoneId(job.getZoneId());
    a.setOriginalScheduledTime(job.getScheduledTime());
    a.setOriginalCreatedAt(job.getCreatedAt());
    a.setFirstExecutionTime(job.getExecutionStartTime());
    a.setCompletionTime(job.getExecutionEndTime());
    a.setTotalExecutionTimeMs(job.getExecutionDurationMs());
    a.setQueueWaitMs(job.getQueueWaitMs());
    a.setArchivedAt(Instant.now());
    a.setArchivedBy(archivedBy);
    a.setArchiveReason(reason);
    a.setJobResult(job.getJobResult());
    a.setResultType(job.getResultType());
    a.setFinalError(job.getLastError());
    if (job.getPayload() != null) {
      a.setPayloadSummary(job.getPayload().target() + "#" + job.getPayload().method());
    }
    a.setDependedOn(job.getDependsOn());
    a.setSupersededBy(job.getSupersededBy());
    if (job.getTags() != null && !job.getTags().isEmpty()) {
      a.setTags(String.join(",", job.getTags()));
    }
    return a;
  }

  private record ClaimCandidate(long id, int priority, Date dueAt) {}
}
