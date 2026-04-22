package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static run.ratchet.store.mongodb.MongoFieldNames.*;

import com.mongodb.MongoCommandException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private final MongoTagOperations tags;
  private final MongoJobCrudOperations crud;
  private final MongoBatchOperations batches;
  private final MongoJobClaimOperations claims;
  private final MongoJobLifecycleOperations lifecycle;

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
    this.tags = new MongoTagOperations(ctx);
    this.crud = new MongoJobCrudOperations(ctx);
    this.batches = new MongoBatchOperations(ctx);
    this.claims = new MongoJobClaimOperations(ctx, claimExecutor);
    this.lifecycle = new MongoJobLifecycleOperations(ctx, batches);
    options.node().explicitTsidNodeId().ifPresent(TsidFactory::configureNodeId);
  }

  @Override
  public JobEntity save(JobEntity job) {
    return crud.save(job);
  }

  @Override
  public Optional<JobEntity> findById(long id) {
    return crud.findById(id);
  }

  @Override
  public Optional<JobEntity> findByIdLatest(long id) {
    return crud.findByIdLatest(id);
  }

  @Override
  public void delete(long id) {
    crud.delete(id);
  }

  @Override
  public JobStatus getJobStatus(long id) {
    return crud.getJobStatus(id);
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    return crud.findByIds(ids);
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    return crud.findActiveByBusinessKey(businessKey);
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    return crud.findByIdempotencyKey(idempotencyKey);
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    return crud.findDependants(parentJobId);
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return crud.findEarliestRecurringNextFire();
  }

  @Override
  public long countPendingJobs() {
    return crud.countPendingJobs();
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return crud.countJobsByStatus(status);
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return crud.countActiveJobs(jobType);
  }

  @Override
  public long countActiveNodes() {
    return crud.countActiveNodes();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return crud.countReadyJobs(now);
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return crud.countStuckJobs(stuckThreshold);
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return crud.countLongRunningJobs(threshold);
  }

  @Override
  public long countPendingBatchChildren() {
    return crud.countPendingBatchChildren();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return crud.countPendingJobsByPriority(priority);
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return crud.countPendingJobsByType(jobType);
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return crud.countJobsByStatusSince(status, since);
  }

  @Override
  public long countJobsWithRetries() {
    return crud.countJobsWithRetries();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    return crud.getRetryRateStats(since);
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    return crud.getAverageProcessingTime(since);
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    return crud.getAverageBatchSize(since);
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    return crud.getOldestPendingJobTime();
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    return crud.getQueueWaitTimePercentile(percentile);
  }

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    return claims.claimNextBatch(limit, nodeId);
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    return claims.claimNextBatchOptimized(jobType, limit, nodeId);
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    return claims.claimDueRecurring(limit, nodeId);
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    lifecycle.updateJobStatus(id, status, errorMessage);
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    return lifecycle.compareAndSwapStatus(id, expected, newStatus, error);
  }

  @Override
  public int incrementRetryAttempt(long id) {
    return lifecycle.incrementRetryAttempt(id);
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    return lifecycle.tryPickUpJob(id, nodeId);
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
    return lifecycle.markJobSucceeded(
        id, resultJson, resultType, start, end, durationMs, queueWaitMs);
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return lifecycle.markJobSucceededMinimal(id, start, end, durationMs, queueWaitMs);
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
    return lifecycle.markJobSucceededAndUpdateBatch(
        jobId, resultJson, resultType, start, end, durationMs, queueWaitMs, batchId);
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    return lifecycle.scheduleJobRetry(id, error, newScheduledTime, attempts);
  }

  @Override
  public boolean pauseRecurring(long id) {
    return lifecycle.pauseRecurring(id);
  }

  @Override
  public boolean resumeRecurring(long id) {
    return lifecycle.resumeRecurring(id);
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    return lifecycle.markJobFailedTerminal(id, terminalError, totalAttempts);
  }

  @Override
  public boolean cancelJob(long id) {
    return lifecycle.cancelJob(id);
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    return lifecycle.resetRunningJob(id, nodeId);
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return lifecycle.resetRunningJobs(nodeId);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return lifecycle.cancelRecurringJobsByTag(tag);
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return lifecycle.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return lifecycle.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }

  @Override
  public boolean resetFailedToPending(long id) {
    return lifecycle.resetFailedToPending(id);
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    return lifecycle.transitionToPaused(id, expected);
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    return lifecycle.transitionFromPaused(id, target);
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    return lifecycle.transitionFromPausedAtomic(id);
  }

  @Override
  public void bulkInsert(List<JobEntity> jobList) {
    crud.bulkInsert(jobList);
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    return crud.deleteJobsByIds(ids);
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return crud.deleteDlqOlderThan(cutoff);
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return crud.resetOrphanJobs(grace);
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return crud.resetOrphanJobsForNode(nodeId);
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    return batches.saveBatch(batch);
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    return batches.findBatchById(batchId);
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    return batches.findBatchesByIds(batchIds);
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    return batches.incrementCompletedAtomic(batchId);
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    return batches.incrementFailedAtomic(batchId);
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    return batches.markBatchCompleteIfReady(batchId);
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    return batches.findRecoverableBatchIds(limit);
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    return batches.updateBatchTotalItems(batchId, totalItems);
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
  public void insertTags(long jobId, List<String> tagList) {
    tags.insertTags(jobId, tagList);
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return tags.deleteTagsByJobId(jobId);
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    return tags.findJobIdsByTag(tag, limit, offset);
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    return tags.countJobsByStatusForTag(tag);
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return tags.countJobsByParamForTag(tag, paramKey);
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return tags.countJobsByExecutionNodeForTag(tag);
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
    return batches.saveBatchMetrics(metrics);
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    return batches.findBatchMetrics(batchId);
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    batches.addChildExecutionTime(batchId, durationMs);
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    batches.finalizeBatchMetrics(batchId);
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    batches.updateBatchMetricsChildCount(batchId, childCount);
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
