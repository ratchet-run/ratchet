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

import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
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
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.util.PriorityBoostConfig;
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
 * MongoDB implementation of the {@link JobStore} SPI.
 *
 * <p>Uses the MongoDB sync driver directly (no ODM). All state transitions use atomic {@code
 * findOneAndUpdate} operations. Tags are embedded in the job document as an array. IDs are
 * generated via {@link run.ratchet.store.id.TsidFactory}.
 */
@ApplicationScoped
public class MongoJobStore implements JobStore {

  private static final Logger log = Logger.getLogger(MongoJobStore.class);
  private static final MongoConstraintDetector CONSTRAINT_DETECTOR = new MongoConstraintDetector();

  private static final List<String> EXECUTABLE_JOB_TYPES =
      List.of("SINGLE", "BATCH_CHILD", "CHAIN_STEP", "WORKFLOW_BRANCH");
  private static final List<String> ACTIVE_STATUSES = List.of("PENDING", "RUNNING", "PAUSED");
  private static final List<String> TERMINAL_STATUSES = List.of("SUCCEEDED", "FAILED", "CANCELED");
  private static final int PRIORITY_BOOST_INTERVAL =
      PriorityBoostConfig.getPriorityBoostIntervalMinutes();
  private static final int CLAIM_CANDIDATE_MULTIPLIER = 8;
  private static final int CLAIM_CANDIDATE_FLOOR = 256;

  /**
   * Upper bound on the priority-boost candidate window. Boost is best-effort above this
   * threshold: once PENDING backlog for a single type exceeds {@value}, older low-raw-priority
   * jobs may not surface for the boost pass because the candidate-selection query caps at this
   * count. Top-priority jobs are still claimed first, but starvation of the oldest low-priority
   * jobs is possible under sustained backlog beyond this ceiling — this is the documented
   * contract, not a bug. Operators who run consistently hot should either raise concurrency or
   * shed low-priority work at enqueue time.
   */
  private static final int CLAIM_CANDIDATE_CEILING = 2048;

  private final MongoDatabase database;
  private final ExecutorService claimExecutor =
      Executors.newFixedThreadPool(
          Math.max(2, Runtime.getRuntime().availableProcessors()),
          r -> {
            Thread t = new Thread(r, "ratchet-mongo-claim");
            t.setDaemon(true);
            return t;
          });

  @Inject
  public MongoJobStore(MongoDatabase database) {
    this.database = database;
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
      jobs().insertOne(doc);
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
        jobs()
            .replaceOne(
                and(eq("_id", job.getId()), eq("version", expectedVersion)),
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
    Document doc = jobs().find(eq("_id", id)).first();
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
    jobs().deleteOne(eq("_id", id));
  }

  @Override
  public JobStatus getJobStatus(long id) {
    Document doc = jobs().find(eq("_id", id)).projection(new Document("status", 1)).first();
    if (doc == null) {
      return null;
    }
    return JobStatus.valueOf(doc.getString("status"));
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : jobs().find(in("_id", ids))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    Document doc =
        jobs()
            .find(and(eq("business_key", businessKey), in("status", ACTIVE_STATUSES)))
            .limit(1)
            .first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    Document doc = jobs().find(eq("idempotency_key", idempotencyKey)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobEntity(doc));
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc : jobs().find(eq("depends_on", parentJobId))) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    Document doc =
        jobs()
            .find(and(eq("job_type", "RECURRING"), eq("status", "PENDING"), ne("next_fire", null)))
            .sort(ascending("next_fire"))
            .projection(new Document("next_fire", 1))
            .limit(1)
            .first();
    if (doc == null || doc.getDate("next_fire") == null) {
      return Optional.empty();
    }
    return Optional.of(DocumentMapper.toInstant(doc.getDate("next_fire")));
  }

  @Override
  public long countPendingJobs() {
    return jobs().countDocuments(eq("status", "PENDING"));
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return jobs().countDocuments(eq("status", status.name()));
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return jobs()
        .countDocuments(
            and(eq("job_type", jobType.name()), in("status", List.of("PENDING", "RUNNING"))));
  }

  @Override
  public long countActiveNodes() {
    return nodes().countDocuments();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return jobs()
        .countDocuments(
            and(eq("status", "PENDING"), lte("scheduled_time", DocumentMapper.toDate(now))));
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return jobs()
        .countDocuments(
            and(eq("status", "RUNNING"), lt("picked_at", DocumentMapper.toDate(stuckThreshold))));
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return jobs()
        .countDocuments(
            and(
                eq("status", "RUNNING"),
                lt("execution_start_time", DocumentMapper.toDate(threshold))));
  }

  @Override
  public long countPendingBatchChildren() {
    return jobs().countDocuments(and(eq("job_type", "BATCH_CHILD"), eq("status", "PENDING")));
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return jobs().countDocuments(and(eq("status", "PENDING"), eq("priority", priority.ordinal())));
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return jobs().countDocuments(and(eq("status", "PENDING"), eq("job_type", jobType.name())));
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return jobs()
        .countDocuments(
            and(eq("status", status.name()), gte("updated_at", DocumentMapper.toDate(since))));
  }

  @Override
  public long countJobsWithRetries() {
    return jobs().countDocuments(new Document("attempts", new Document("$gt", 0)));
  }

  @Override
  public double getRetryRateStats(Instant since) {
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document("updated_at", new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document("_id", null)
                    .append("total", new Document("$sum", 1))
                    .append(
                        "retried",
                        new Document(
                            "$sum",
                            new Document(
                                "$cond",
                                List.of(new Document("$gt", List.of("$attempts", 0)), 1, 0))))));
    Document result = jobs().aggregate(pipeline).first();
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
                new Document("status", "SUCCEEDED")
                    .append("updated_at", new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document("_id", null)
                    .append("avg", new Document("$avg", "$execution_duration_ms"))));
    Document result = jobs().aggregate(pipeline).first();
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
                    .append("localField", "_id")
                    .append("foreignField", "_id")
                    .append("as", "job")),
            new Document("$unwind", "$job"),
            new Document(
                "$match",
                new Document("job.updated_at", new Document("$gte", DocumentMapper.toDate(since)))),
            new Document(
                "$group",
                new Document("_id", null).append("avg", new Document("$avg", "$total_items"))));
    Document result = batches().aggregate(pipeline).first();
    if (result == null || result.get("avg") == null) {
      return 0.0;
    }
    return ((Number) result.get("avg")).doubleValue();
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    Document doc =
        jobs()
            .find(eq("status", "PENDING"))
            .sort(ascending("scheduled_time"))
            .projection(new Document("scheduled_time", 1))
            .limit(1)
            .first();
    if (doc == null || doc.getDate("scheduled_time") == null) {
      return Optional.empty();
    }
    return Optional.of(DocumentMapper.toInstant(doc.getDate("scheduled_time")));
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    // Use $setWindowFields with $percentile (MongoDB 7.0+), fallback to sort+skip approximation
    List<Document> pipeline =
        List.of(
            new Document(
                "$match",
                new Document("queue_wait_ms", new Document("$ne", null))
                    .append("status", "SUCCEEDED")),
            new Document(
                "$group",
                new Document("_id", null)
                    .append(
                        "p",
                        new Document(
                            "$percentile",
                            new Document("input", "$queue_wait_ms")
                                .append("p", List.of(percentile))
                                .append("method", "approximate")))));
    try {
      Document result = jobs().aggregate(pipeline).first();
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
    long total = jobs().countDocuments(and(ne("queue_wait_ms", null), eq("status", "SUCCEEDED")));
    if (total == 0) {
      return 0;
    }
    long skipCount = (long) (total * percentile);
    Document doc =
        jobs()
            .find(and(ne("queue_wait_ms", null), eq("status", "SUCCEEDED")))
            .sort(ascending("queue_wait_ms"))
            .skip((int) Math.min(skipCount, Integer.MAX_VALUE))
            .limit(1)
            .projection(new Document("queue_wait_ms", 1))
            .first();
    return doc == null || doc.getLong("queue_wait_ms") == null ? 0 : doc.getLong("queue_wait_ms");
  }

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(EXECUTABLE_JOB_TYPES, "scheduled_time", limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !isPollerExecutable(jobType)) {
      return List.of();
    }
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of(jobType.name()), "scheduled_time", limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobClaimDto);
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of("RECURRING"), "next_fire", limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    jobs()
        .updateOne(
            eq("_id", id),
            combine(
                set("status", status.name()),
                set("last_error", errorMessage),
                set("updated_at", DocumentMapper.toDate(Instant.now())),
                inc("version", 1)));
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    try {
      UpdateResult result =
          jobs()
              .updateOne(
                  and(eq("_id", id), eq("status", expected.name())),
                  combine(
                      set("status", newStatus.name()),
                      set("last_error", error),
                      set("updated_at", DocumentMapper.toDate(Instant.now())),
                      inc("version", 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("compare-and-swap status", e);
    }
  }

  @Override
  public int incrementRetryAttempt(long id) {
    Document doc =
        jobs()
            .findOneAndUpdate(
                and(eq("_id", id), eq("status", "RUNNING")),
                combine(
                    inc("attempts", 1),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      return -1;
    }
    return doc.getInteger("attempts");
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    Instant now = Instant.now();
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", "PENDING")),
                combine(
                    set("status", "RUNNING"),
                    set("picked_by", nodeId),
                    set("picked_at", DocumentMapper.toDate(now)),
                    set("updated_at", DocumentMapper.toDate(now)),
                    inc("version", 1)));
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
          jobs()
              .updateOne(
                  and(eq("_id", id), eq("status", "RUNNING")),
                  combine(
                      set("status", "SUCCEEDED"),
                      set("job_result", resultJson),
                      set("result_type", resultType),
                      set("execution_start_time", DocumentMapper.toDate(start)),
                      set("execution_end_time", DocumentMapper.toDate(end)),
                      set("execution_duration_ms", durationMs),
                      set("queue_wait_ms", queueWaitMs),
                      set("last_error", null),
                      set("updated_at", DocumentMapper.toDate(Instant.now())),
                      inc("version", 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("mark job succeeded", e);
    }
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      UpdateResult result =
          jobs()
              .updateOne(
                  and(eq("_id", id), eq("status", "RUNNING")),
                  combine(
                      set("status", "SUCCEEDED"),
                      set("execution_start_time", DocumentMapper.toDate(start)),
                      set("execution_end_time", DocumentMapper.toDate(end)),
                      set("execution_duration_ms", durationMs),
                      set("queue_wait_ms", queueWaitMs),
                      set("last_error", null),
                      set("updated_at", DocumentMapper.toDate(Instant.now())),
                      inc("version", 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("mark job succeeded minimally", e);
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
        jobs()
            .updateOne(
                and(eq("_id", id), in("status", List.of("RUNNING", "FAILED"))),
                combine(
                    set("status", "PENDING"),
                    set("scheduled_time", DocumentMapper.toDate(newScheduledTime)),
                    set("attempts", attempts),
                    set("last_error", error),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean pauseRecurring(long id) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("job_type", "RECURRING"), eq("status", "PENDING")),
                combine(
                    set("status", "PAUSED"),
                    set("paused_from_status", "PENDING"),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean resumeRecurring(long id) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("job_type", "RECURRING"), eq("status", "PAUSED")),
                combine(
                    set("status", "PENDING"),
                    set("paused_from_status", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", "RUNNING")),
                combine(
                    set("status", "FAILED"),
                    set("last_error", terminalError),
                    set("attempts", totalAttempts),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean cancelJob(long id) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), in("status", List.of("PENDING", "RUNNING", "PAUSED"))),
                combine(
                    set("status", "CANCELED"),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", "RUNNING"), eq("picked_by", nodeId)),
                combine(
                    set("status", "PENDING"),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    UpdateResult result =
        jobs()
            .updateMany(
                and(eq("status", "RUNNING"), eq("picked_by", nodeId)),
                combine(
                    set("status", "PENDING"),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    UpdateResult result =
        jobs()
            .updateMany(
                and(eq("tags", tag), eq("job_type", "RECURRING"), in("status", ACTIVE_STATUSES)),
                combine(
                    set("status", "CANCELED"),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    UpdateResult result =
        jobs()
            .updateMany(
                and(
                    eq("business_key", businessKey),
                    eq("job_type", "RECURRING"),
                    in("status", ACTIVE_STATUSES)),
                combine(
                    set("status", "CANCELED"),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    UpdateResult result =
        jobs()
            .updateMany(
                and(
                    eq("job_type", "RECURRING"),
                    in("status", ACTIVE_STATUSES),
                    lt("created_at", DocumentMapper.toDate(nodeStartTime)),
                    ne("business_key", null),
                    nin("business_key", registeredIds)),
                combine(
                    set("status", "CANCELED"),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public boolean resetFailedToPending(long id) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", "FAILED")),
                combine(
                    set("status", "PENDING"),
                    set("attempts", 0),
                    set("last_error", null),
                    set("scheduled_time", DocumentMapper.toDate(Instant.now())),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", expected.name())),
                combine(
                    set("status", "PAUSED"),
                    set("paused_from_status", expected.name()),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    UpdateResult result =
        jobs()
            .updateOne(
                and(eq("_id", id), eq("status", "PAUSED")),
                combine(
                    set("status", target.name()),
                    set("paused_from_status", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return result.getModifiedCount() > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    Document before =
        jobs()
            .findOneAndUpdate(
                and(eq("_id", id), eq("status", "PAUSED")),
                List.of(
                    new Document(
                        "$set",
                        new Document()
                            .append(
                                "status",
                                new Document("$ifNull", List.of("$paused_from_status", "PENDING")))
                            .append("paused_from_status", null)
                            .append("updated_at", new Date())
                            .append("version", new Document("$add", List.of("$version", 1))))),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return null;
    }
    String pausedFrom = before.getString("paused_from_status");
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
    jobs().insertMany(docs);
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    DeleteResult result = jobs().deleteMany(in("_id", ids));
    return (int) result.getDeletedCount();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    DeleteResult result =
        jobs()
            .deleteMany(
                and(
                    eq("status", "FAILED"),
                    new Document(
                        "$expr", new Document("$gte", List.of("$attempts", "$max_retries"))),
                    lt("updated_at", DocumentMapper.toDate(cutoff))));
    return (int) result.getDeletedCount();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    // Use Duration directly — toMinutes() truncates sub-minute values
    Date cutoff = DocumentMapper.toDate(Instant.now().minus(grace));

    // Find active node IDs
    List<String> activeNodeIds = new ArrayList<>();
    for (Document doc : nodes().find(gte("heartbeat_ts", cutoff))) {
      activeNodeIds.add(doc.getString("_id"));
    }

    Bson filter;
    if (activeNodeIds.isEmpty()) {
      // All nodes are inactive — reset all running jobs past grace
      filter = and(eq("status", "RUNNING"), lt("picked_at", cutoff));
    } else {
      filter =
          and(eq("status", "RUNNING"), nin("picked_by", activeNodeIds), lt("picked_at", cutoff));
    }

    UpdateResult result =
        jobs()
            .updateMany(
                filter,
                combine(
                    set("status", "PENDING"),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    UpdateResult result =
        jobs()
            .updateMany(
                and(eq("status", "RUNNING"), eq("picked_by", nodeId)),
                combine(
                    set("status", "PENDING"),
                    set("picked_by", null),
                    set("picked_at", null),
                    set("updated_at", DocumentMapper.toDate(Instant.now())),
                    inc("version", 1)));
    return (int) result.getModifiedCount();
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    Document doc = DocumentMapper.toDocument(batch);
    batches().replaceOne(eq("_id", batch.getId()), doc, new ReplaceOptions().upsert(true));
    return batch;
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    Document doc = batches().find(eq("_id", batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchEntity(doc));
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    List<BatchEntity> result = new ArrayList<>();
    for (Document doc : batches().find(in("_id", batchIds))) {
      result.add(DocumentMapper.toBatchEntity(doc));
    }
    return result;
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    Document doc =
        batches()
            .findOneAndUpdate(
                eq("_id", batchId),
                inc("completed_items", 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    Document doc =
        batches()
            .findOneAndUpdate(
                eq("_id", batchId),
                inc("failed_items", 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    UpdateResult result =
        batches()
            .updateOne(
                and(
                    eq("_id", batchId),
                    eq("completion_processed", false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document("$add", List.of("$completed_items", "$failed_items")),
                                "$total_items")))),
                set("completion_processed", true));
    return result.getModifiedCount() > 0;
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    List<Long> ids = new ArrayList<>();
    FindIterable<Document> results =
        batches()
            .find(
                and(
                    eq("completion_processed", false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document("$add", List.of("$completed_items", "$failed_items")),
                                "$total_items")))))
            .projection(new Document("_id", 1))
            .limit(limit);
    for (Document doc : results) {
      ids.add(doc.getLong("_id"));
    }
    return ids;
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    UpdateResult result = batches().updateOne(eq("_id", batchId), set("total_items", totalItems));
    return result.getModifiedCount() > 0;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    Date now = DocumentMapper.toDate(Instant.now());
    Date expiresAt = DocumentMapper.toDate(Instant.now().plus(ttl));

    try {
      // Attempt to upsert: insert if no lock exists, or update if lock is expired
      Document result =
          locks()
              .findOneAndUpdate(
                  and(eq("_id", name), lt("expires_at", now)),
                  combine(
                      set("owner_node", nodeId),
                      set("locked_at", now),
                      set("expires_at", expiresAt),
                      setOnInsert("_id", name)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

      // If we got a result with our nodeId, the lock was acquired (insert or expired-update)
      return result != null && nodeId.equals(result.getString("owner_node"));
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
    locks().deleteOne(and(eq("_id", name), eq("owner_node", nodeId)));
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    Date newExpiry = DocumentMapper.toDate(Instant.now().plus(extension));
    UpdateResult result =
        locks()
            .updateOne(
                and(eq("_id", name), eq("owner_node", nodeId)), set("expires_at", newExpiry));
    return result.getModifiedCount() > 0;
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    Date tsDate = DocumentMapper.toDate(ts);
    nodes()
        .updateOne(
            eq("_id", nodeId),
            combine(set("heartbeat_ts", tsDate), setOnInsert("started_at", tsDate)),
            new UpdateOptions().upsert(true));
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    Document doc = nodes().find(eq("_id", nodeId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toNodeEntity(doc));
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    List<NodeEntity> results = new ArrayList<>();
    for (Document doc : nodes().find(lt("heartbeat_ts", DocumentMapper.toDate(cutoff)))) {
      results.add(DocumentMapper.toNodeEntity(doc));
    }
    return results;
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    DeleteResult result = nodes().deleteMany(lt("heartbeat_ts", DocumentMapper.toDate(cutoff)));
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
    archives().insertOne(doc);
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
    archives().insertMany(docs);
    return docs.size();
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc :
        jobs()
            .find(
                and(
                    in("status", TERMINAL_STATUSES),
                    lt("updated_at", DocumentMapper.toDate(olderThan))))
            .sort(ascending("updated_at"))
            .limit(limit)) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return jobs()
        .countDocuments(
            and(
                in("status", TERMINAL_STATUSES),
                lt("updated_at", DocumentMapper.toDate(olderThan))));
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    List<Bson> filters = new ArrayList<>();
    if (targetClass != null) {
      filters.add(eq("target_class", targetClass));
    }
    if (businessKey != null) {
      filters.add(eq("business_key", businessKey));
    }
    if (from != null) {
      filters.add(gte("archived_at", DocumentMapper.toDate(from)));
    }
    if (to != null) {
      filters.add(lte("archived_at", DocumentMapper.toDate(to)));
    }

    Bson filter = filters.isEmpty() ? new Document() : and(filters);
    List<ArchivedJobEntity> results = new ArrayList<>();
    for (Document doc : archives().find(filter).sort(descending("archived_at")).limit(limit)) {
      results.add(DocumentMapper.toArchivedJobEntity(doc));
    }
    return results;
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    DeleteResult result =
        archives().deleteMany(lt("archived_at", DocumentMapper.toDate(olderThan)));
    return (int) result.getDeletedCount();
  }

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      execution.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(execution);
    executions().replaceOne(eq("_id", execution.getId()), doc, new ReplaceOptions().upsert(true));
    return execution;
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    List<JobExecutionEntity> results = new ArrayList<>();
    for (Document doc : executions().find(eq("job_id", jobId)).sort(ascending("attempt"))) {
      results.add(DocumentMapper.toJobExecutionEntity(doc));
    }
    return results;
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    Document doc =
        executions().find(eq("job_id", jobId)).sort(descending("attempt")).limit(1).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobExecutionEntity(doc));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return (int) executions().countDocuments(eq("job_id", jobId));
  }

  @Override
  public void appendLog(JobLogEntity logEntry) {
    if (logEntry.getId() == null) {
      logEntry.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(logEntry);
    jobLogs().insertOne(doc);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    DeleteResult result = jobLogs().deleteMany(lt("ts", DocumentMapper.toDate(cutoff)));
    return (int) result.getDeletedCount();
  }

  @Override
  public void insertTags(long jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    // Tags are embedded in job document — use $addToSet with $each
    jobs()
        .updateOne(
            eq("_id", jobId),
            new Document("$addToSet", new Document("tags", new Document("$each", tags))));
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    Document before =
        jobs()
            .findOneAndUpdate(
                eq("_id", jobId),
                set("tags", List.of()),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return 0;
    }
    List<String> oldTags = before.getList("tags", String.class);
    return oldTags == null ? 0 : oldTags.size();
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    List<Long> ids = new ArrayList<>();
    for (Document doc :
        jobs()
            .find(eq("tags", tag))
            .projection(new Document("_id", 1))
            .sort(ascending("_id"))
            .skip(offset)
            .limit(limit)) {
      ids.add(doc.getLong("_id"));
    }
    return ids;
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Document doc :
        jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document("tags", tag)),
                    new Document("$group", new Document("_id", "$status").append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document("_id", 1))))) {
      String status = doc.getString("_id");
      if (status != null) {
        counts.put(JobStatus.valueOf(status), ((Number) doc.get("count")).longValue());
      }
    }
    return counts;
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return aggregateStringCountsByTag(
        tag,
        new Document(
            "$getField", new Document("field", paramKey).append("input", "$params")));
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return aggregateStringCountsByTag(tag, "$picked_by");
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
    workflowConditions()
        .replaceOne(eq("_id", condition.getId()), doc, new ReplaceOptions().upsert(true));
    return condition;
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    Document doc = workflowConditions().find(eq("_id", id)).first();
    return doc == null ? null : DocumentMapper.toWorkflowConditionEntity(doc);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        workflowConditions()
            .find(eq("parent_job_id", parentJobId))
            .sort(ascending("condition_priority"))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc : workflowConditions().find(eq("child_job_id", childJobId))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        workflowConditions()
            .find(and(eq("parent_job_id", parentJobId), eq("condition_type", type.name())))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
    }
    return results;
  }

  @Override
  public void deleteConditionById(long id) {
    workflowConditions().deleteOne(eq("_id", id));
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    workflowConditions().deleteMany(eq("parent_job_id", parentJobId));
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    workflowConditions().deleteMany(eq("child_job_id", childJobId));
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return workflowConditions().countDocuments(eq("parent_job_id", parentJobId));
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    Document doc = DocumentMapper.toDocument(metrics);
    batchMetrics()
        .replaceOne(eq("_id", metrics.getBatchId()), doc, new ReplaceOptions().upsert(true));
    return metrics;
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    Document doc = batchMetrics().find(eq("_id", batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchMetricsEntity(doc));
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    batchMetrics()
        .updateOne(
            eq("_id", batchId),
            combine(inc("child_execution_ms", durationMs), inc("success_count", 1)));
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    Document doc = batchMetrics().find(eq("_id", batchId)).first();
    if (doc == null) {
      return;
    }
    Instant now = Instant.now();
    Date startedAt = doc.getDate("started_at");
    Long childExecutionMs = doc.getLong("child_execution_ms");

    Long totalDurationMs = null;
    Long overheadMs = null;
    if (startedAt != null) {
      totalDurationMs = Duration.between(startedAt.toInstant(), now).toMillis();
      if (childExecutionMs != null) {
        overheadMs = totalDurationMs - childExecutionMs;
      }
    }

    batchMetrics()
        .updateOne(
            eq("_id", batchId),
            combine(
                set("completed_at", DocumentMapper.toDate(now)),
                set("total_duration_ms", totalDurationMs),
                set("overhead_ms", overheadMs)));
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    batchMetrics().updateOne(eq("_id", batchId), set("child_count", childCount));
  }

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    if (alert.getId() == null) {
      alert.setId(TsidFactory.next());
    }
    Document doc = DocumentMapper.toDocument(alert);
    dlqAlerts().replaceOne(eq("_id", alert.getId()), doc, new ReplaceOptions().upsert(true));
    return alert;
  }

  @Override
  public boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff) {
    return dlqAlerts()
            .countDocuments(
                and(
                    eq("job_id", jobId),
                    eq("error_hash", errorHash),
                    gte("alert_sent_at", DocumentMapper.toDate(cutoff))))
        > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    // Atomically increment active_count only if it is below max_concurrent.
    // Uses $expr to compare two fields in the same document, ensuring no TOCTOU race.
    Document result =
        resourceLimits()
            .findOneAndUpdate(
                and(
                    eq("_id", resource),
                    expr(
                        new Document(
                            "$lt",
                            List.of(
                                new Document("$ifNull", List.of("$active_count", 0)),
                                "$max_concurrent")))),
                inc("active_count", 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

    if (result == null) {
      return false;
    }

    ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
    permit.setId(TsidFactory.next());
    resourcePermits().insertOne(DocumentMapper.toDocument(permit));
    return true;
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    DeleteResult dr =
        resourcePermits().deleteOne(and(eq("resource_name", resource), eq("job_id", jobId)));
    if (dr.getDeletedCount() > 0) {
      resourceLimits().updateOne(eq("_id", resource), inc("active_count", -1));
    }
  }

  @Override
  public void releaseAllPermits(long jobId) {
    List<String> resources = new ArrayList<>();
    resourcePermits()
        .find(eq("job_id", jobId))
        .forEach(doc -> resources.add(doc.getString("resource_name")));
    DeleteResult dr = resourcePermits().deleteMany(eq("job_id", jobId));
    if (dr.getDeletedCount() > 0) {
      for (String resource : resources) {
        resourceLimits().updateOne(eq("_id", resource), inc("active_count", -1));
      }
    }
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    Document doc = resourceLimits().find(eq("_id", resource)).first();
    if (doc == null) {
      return 5000;
    }
    return doc.getInteger("retry_delay_ms", 5000);
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    Instant now = Instant.now();
    resourceLimits()
        .updateOne(
            eq("_id", name),
            combine(
                set("max_concurrent", maxConcurrent),
                set("retry_delay_ms", retryDelayMs),
                set("description", description),
                set("updated_at", DocumentMapper.toDate(now)),
                setOnInsert("created_at", DocumentMapper.toDate(now)),
                setOnInsert("active_count", 0)),
            new UpdateOptions().upsert(true));
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    List<Document> orphanedPermits = new ArrayList<>();
    resourcePermits().find(in("node_id", staleNodeIds)).forEach(orphanedPermits::add);
    DeleteResult result = resourcePermits().deleteMany(in("node_id", staleNodeIds));
    orphanedPermits.stream()
        .map(doc -> doc.getString("resource_name"))
        .distinct()
        .forEach(
            resource -> {
              long count =
                  orphanedPermits.stream()
                      .filter(doc -> resource.equals(doc.getString("resource_name")))
                      .count();
              resourceLimits().updateOne(eq("_id", resource), inc("active_count", (int) -count));
            });
    return (int) result.getDeletedCount();
  }

  @PostConstruct
  void initializeCollections() {
    new MongoCollectionInitializer(database).initialize();
  }

  private MongoCollection<Document> jobs() {
    return database.getCollection("scheduler_job");
  }

  private MongoCollection<Document> batches() {
    return database.getCollection("scheduler_batch");
  }

  private MongoCollection<Document> batchMetrics() {
    return database.getCollection("scheduler_batch_metrics");
  }

  private MongoCollection<Document> executions() {
    return database.getCollection("scheduler_job_execution");
  }

  private MongoCollection<Document> jobLogs() {
    return database.getCollection("scheduler_job_log");
  }

  private MongoCollection<Document> archives() {
    return database.getCollection("scheduler_job_archive");
  }

  private MongoCollection<Document> locks() {
    return database.getCollection("scheduler_lock");
  }

  private MongoCollection<Document> nodes() {
    return database.getCollection("scheduler_node");
  }

  private MongoCollection<Document> workflowConditions() {
    return database.getCollection("scheduler_workflow_condition");
  }

  private MongoCollection<Document> dlqAlerts() {
    return database.getCollection("scheduler_dlq_alerts");
  }

  private MongoCollection<Document> resourceLimits() {
    return database.getCollection("scheduler_resource_limit");
  }

  private MongoCollection<Document> resourcePermits() {
    return database.getCollection("scheduler_resource_permit");
  }

  /** Finds candidate job IDs sorted by effective priority (raw priority + age-based boost). */
  private List<Long> findCandidatesByBoostedPriority(
      List<String> jobTypes, String timeColumn, int limit) {
    if (limit <= 0 || jobTypes.isEmpty()) {
      return List.of();
    }

    Date now = DocumentMapper.toDate(Instant.now());
    int candidateWindow = computeCandidateWindow(limit);
    List<ClaimCandidate> candidates = new ArrayList<>(candidateWindow * jobTypes.size());

    for (String jobType : jobTypes) {
      candidates.addAll(findCandidatesForJobType(jobType, timeColumn, now, candidateWindow));
    }

    if (candidates.isEmpty()) {
      return List.of();
    }

    candidates.sort(
        (left, right) -> {
          int priorityComparison =
              Integer.compare(
                  effectivePriority(right, now.getTime()), effectivePriority(left, now.getTime()));
          if (priorityComparison != 0) {
            return priorityComparison;
          }
          int timeComparison = left.dueAt().compareTo(right.dueAt());
          if (timeComparison != 0) {
            return timeComparison;
          }
          return Long.compare(left.id(), right.id());
        });

    List<Long> ids = new ArrayList<>(Math.min(limit, candidates.size()));
    for (ClaimCandidate candidate : candidates) {
      if (ids.size() >= limit) {
        break;
      }
      ids.add(candidate.id());
    }
    return ids;
  }

  private List<ClaimCandidate> findCandidatesForJobType(
      String jobType, String timeColumn, Date now, int candidateWindow) {
    Bson filter =
        and(eq("status", "PENDING"), eq("job_type", jobType), lte(timeColumn, now));
    Bson projection =
        new Document("_id", 1).append("priority", 1).append(timeColumn, 1);
    // We want priority DESC, then due time ASC, then _id ASC.
    Bson sort = new Document("priority", -1).append(timeColumn, 1).append("_id", 1);

    FindIterable<Document> query =
        jobs().find(filter).sort(sort).projection(projection).limit(candidateWindow);

    if ("next_fire".equals(timeColumn)) {
      query.hintString("idx_job_claim_recurring");
    } else {
      query.hintString("idx_job_claim_exec");
    }

    List<ClaimCandidate> candidates = new ArrayList<>();
    for (Document doc : query) {
      Date dueAt = doc.getDate(timeColumn);
      if (dueAt == null) {
        continue;
      }
      candidates.add(
          new ClaimCandidate(
              doc.getLong("_id"),
              doc.getInteger("priority", JobPriority.NORMAL.ordinal()),
              dueAt));
    }
    return candidates;
  }

  private static int computeCandidateWindow(int limit) {
    long scaled = (long) limit * CLAIM_CANDIDATE_MULTIPLIER;
    long bounded = Math.max(CLAIM_CANDIDATE_FLOOR, scaled);
    return (int) Math.min(CLAIM_CANDIDATE_CEILING, bounded);
  }

  private static int effectivePriority(ClaimCandidate candidate, long nowMillis) {
    if (PRIORITY_BOOST_INTERVAL <= 0) {
      return candidate.priority();
    }
    long ageMillis = nowMillis - candidate.dueAt().getTime();
    if (ageMillis <= 0) {
      return candidate.priority();
    }
    long boost = (ageMillis / 60_000L) / PRIORITY_BOOST_INTERVAL;
    long effective = (long) candidate.priority() + boost;
    return effective > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) effective;
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
                            jobs()
                                .findOneAndUpdate(
                                    and(eq("_id", id), eq("status", "PENDING")),
                                    combine(
                                        set("status", "RUNNING"),
                                        set("picked_by", nodeId),
                                        set("picked_at", nowDate),
                                        set("updated_at", nowDate),
                                        inc("version", 1)),
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

  private static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  private record ClaimCandidate(long id, int priority, Date dueAt) {}

  private Map<String, Long> aggregateStringCountsByTag(String tag, Object groupExpression) {
    Map<String, Long> counts = new TreeMap<>();
    for (Document doc :
        jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document("tags", tag)),
                    new Document(
                        "$group",
                        new Document("_id", groupExpression)
                            .append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document("_id", 1))))) {
      Object keyValue = doc.get("_id");
      if (!(keyValue instanceof String key) || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) doc.get("count")).longValue());
    }
    return counts;
  }

  private RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (CONSTRAINT_DETECTOR.isDeadlock(e) || CONSTRAINT_DETECTOR.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient MongoDB store concurrency failure during " + operation, e);
    }
    return e;
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
}
