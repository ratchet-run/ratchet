package run.ratchet.store.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.RatchetConfigurationException;
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
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;

/**
 * MongoDB implementation of the {@link MongoJobStore} API.
 *
 * <p>Uses the MongoDB sync driver directly (no ODM). All state transitions use atomic {@code
 * findOneAndUpdate} operations. Tags are embedded in the job document as an array. IDs are UUIDv7
 * values assigned by {@link run.ratchet.store.id.UuidV7EntityListener} on persist.
 */
@ApplicationScoped
class MongoJobStoreImpl implements MongoJobStore {

  private final MongoDatabase database;
  private final MongoStoreContext ctx;
  private final MongoTagOperations tags;
  private final MongoJobCrudOperations crud;
  private final MongoBatchOperations batches;
  private final MongoJobClaimOperations claims;
  private final MongoJobLifecycleOperations lifecycle;
  private final MongoNodeLockOperations nodeLocks;
  private final MongoArchiveOperations archives;
  private final MongoAuxiliaryOperations auxiliary;
  private final MongoJobQueryOperations query;
  private final MongoSignalOperations signals;
  private final MongoRecurringJobOperations recurringJobs;

  MongoJobStoreImpl(MongoClient client, MongoDatabase database, RatchetOptions options) {
    this(client, database, options, MongoStoreContext.noopMetricsCollector());
  }

  @Inject
  MongoJobStoreImpl(
      MongoClient client,
      MongoDatabase database,
      RatchetOptions options,
      MetricsCollector metricsCollector) {
    this.database = database;
    // Mongo operation delegates are pure synchronous wrappers around the injected client/database.
    // Startup validation and index creation remain in @PostConstruct because they touch the server.
    this.ctx =
        new MongoStoreContext(
            client, database, metricsCollector, options.store().priorityBoostIntervalMinutes());
    this.tags = new MongoTagOperations(ctx);
    this.crud = new MongoJobCrudOperations(ctx);
    this.batches = new MongoBatchOperations(ctx);
    this.claims = new MongoJobClaimOperations(ctx);
    this.lifecycle = new MongoJobLifecycleOperations(ctx, batches);
    this.nodeLocks = new MongoNodeLockOperations(ctx);
    this.archives = new MongoArchiveOperations(ctx);
    this.auxiliary = new MongoAuxiliaryOperations(ctx);
    this.query = new MongoJobQueryOperations(ctx);
    this.signals = new MongoSignalOperations(ctx);
    this.recurringJobs = new MongoRecurringJobOperations(ctx);
  }

  @Override
  public JobEntity create(JobEntity job) {
    return crud.create(job);
  }

  @Override
  public JobEntity save(JobEntity job) {
    return crud.save(job);
  }

  @Override
  public Optional<JobEntity> findById(UUID id) {
    return crud.findById(id);
  }

  @Override
  public Optional<JobEntity> findByIdLatest(UUID id) {
    return crud.findByIdLatest(id);
  }

  @Override
  public void delete(UUID id) {
    crud.delete(id);
  }

  @Override
  public JobStatus getJobStatus(UUID id) {
    return crud.getJobStatus(id);
  }

  @Override
  public List<JobEntity> findByIds(List<UUID> ids) {
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
  public List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    return crud.findDependants(parentJobId, limit, offset);
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return recurringJobs.findEarliestRecurringNextFire();
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
  public Map<JobStatus, Long> countJobsByStatuses() {
    return crud.countJobsByStatuses();
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
  public Map<JobPriority, Long> countPendingJobsByPriorities() {
    return crud.countPendingJobsByPriorities();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return crud.countPendingJobsByType(jobType);
  }

  @Override
  public Map<JobExecutionType, Long> countPendingJobsByTypes() {
    return crud.countPendingJobsByTypes();
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
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    return claims.claimNextBatch(limit, nodeId, tagFilter);
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter) {
    return claims.claimNextBatchOptimized(jobType, limit, nodeId, tagFilter);
  }

  @Override
  public void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    lifecycle.updateJobStatus(id, status, errorMessage);
  }

  @Override
  public boolean compareAndSwapStatus(
      UUID id, JobStatus expected, JobStatus newStatus, String error) {
    return lifecycle.compareAndSwapStatus(id, expected, newStatus, error);
  }

  @Override
  public int incrementRetryAttempt(UUID id) {
    return lifecycle.incrementRetryAttempt(id);
  }

  @Override
  public boolean tryPickUpJob(UUID id, String nodeId) {
    return lifecycle.tryPickUpJob(id, nodeId);
  }

  @Override
  public boolean markJobSucceeded(
      UUID id,
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
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return lifecycle.markJobSucceededMinimal(id, start, end, durationMs, queueWaitMs);
  }

  @Override
  public boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId) {
    return lifecycle.markJobSucceededAndUpdateBatch(
        jobId, resultJson, resultType, start, end, durationMs, queueWaitMs, batchId);
  }

  @Override
  public boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    return lifecycle.scheduleJobRetry(id, error, newScheduledTime, attempts);
  }

  @Override
  public boolean pauseRecurring(UUID id) {
    return recurringJobs.pauseRecurring(id);
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    return recurringJobs.resumeRecurring(id);
  }

  @Override
  public boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return lifecycle.markJobFailedTerminal(id, terminalError, totalAttempts);
  }

  @Override
  public boolean cancelJob(UUID id) {
    return lifecycle.cancelJob(id);
  }

  @Override
  public boolean resetRunningJob(UUID id, String nodeId) {
    return lifecycle.resetRunningJob(id, nodeId);
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return lifecycle.resetRunningJobs(nodeId);
  }

  @Override
  public int cancelJobsByTag(String tag) {
    return lifecycle.cancelJobsByTag(tag);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return recurringJobs.cancelRecurringJobsByTag(tag);
  }

  @Override
  public int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
    return recurringJobs.cancelRecurringJobsByBusinessKeys(businessKeys);
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return recurringJobs.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return lifecycle.resetFailedToPending(id);
  }

  @Override
  public boolean transitionToPaused(UUID id, JobStatus expected) {
    return lifecycle.transitionToPaused(id, expected);
  }

  @Override
  public boolean transitionFromPaused(UUID id, JobStatus target) {
    return lifecycle.transitionFromPaused(id, target);
  }

  @Override
  public JobStatus transitionFromPausedAtomic(UUID id) {
    return lifecycle.transitionFromPausedAtomic(id);
  }

  @Override
  public void bulkInsert(List<JobEntity> jobList) {
    crud.bulkInsert(jobList);
  }

  @Override
  public int deleteJobsByIds(List<UUID> ids) {
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
  public int resetOrphanJobsBefore(Instant cutoff) {
    return crud.resetOrphanJobsBefore(cutoff);
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
  public Optional<BatchEntity> findBatchById(UUID batchId) {
    return batches.findBatchById(batchId);
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<UUID> batchIds) {
    return batches.findBatchesByIds(batchIds);
  }

  @Override
  public BatchProgress incrementCompletedAtomic(UUID batchId) {
    return batches.incrementCompletedAtomic(batchId);
  }

  @Override
  public BatchProgress incrementFailedAtomic(UUID batchId) {
    return batches.incrementFailedAtomic(batchId);
  }

  @Override
  public boolean markBatchCompleteIfReady(UUID batchId) {
    return batches.markBatchCompleteIfReady(batchId);
  }

  @Override
  public List<UUID> findRecoverableBatchIds(int limit) {
    return batches.findRecoverableBatchIds(limit);
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    return batches.updateBatchTotalItems(batchId, totalItems);
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    return nodeLocks.tryLock(name, ttl, nodeId);
  }

  @Override
  public void unlock(String name, String nodeId) {
    nodeLocks.unlock(name, nodeId);
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    return nodeLocks.renewLock(name, extension, nodeId);
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    nodeLocks.upsertHeartbeat(nodeId, ts);
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return nodeLocks.findNodeById(nodeId);
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    return nodeLocks.findInactiveNodesSince(cutoff);
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return nodeLocks.deleteInactiveNodesSince(cutoff);
  }

  @Override
  public int deleteInactiveNodesByIds(java.util.Collection<String> nodeIds) {
    return nodeLocks.deleteInactiveNodesByIds(nodeIds);
  }

  @Override
  public Instant getDatabaseTime() {
    return nodeLocks.getDatabaseTime();
  }

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    return archives.archiveJob(job, reason, archivedBy);
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobList, String reason, String archivedBy) {
    return archives.archiveJobsBatch(jobList, reason, archivedBy);
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    return archives.findJobsForArchiving(olderThan, limit);
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return archives.countJobsForArchiving(olderThan);
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    return archives.findArchivedJobs(targetClass, businessKey, from, to, limit);
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return archives.purgeArchivedJobs(olderThan);
  }

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    return auxiliary.saveExecution(execution);
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(UUID jobId, int limit, int offset) {
    return auxiliary.findExecutionsByJobId(jobId, limit, offset);
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(UUID jobId) {
    return auxiliary.findLatestExecution(jobId);
  }

  @Override
  public int countExecutionAttempts(UUID jobId) {
    return auxiliary.countExecutionAttempts(jobId);
  }

  @Override
  public void appendLog(JobLogEntity logEntry) {
    auxiliary.appendLog(logEntry);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return auxiliary.purgeLogsOlderThan(cutoff);
  }

  @Override
  public void insertTags(UUID jobId, List<String> tagList) {
    tags.insertTags(jobId, tagList);
  }

  @Override
  public int deleteTagsByJobId(UUID jobId) {
    return tags.deleteTagsByJobId(jobId);
  }

  @Override
  public List<UUID> findJobIdsByTag(String tag, int limit, int offset) {
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
    return auxiliary.saveCondition(condition);
  }

  @Override
  public WorkflowConditionEntity findConditionById(UUID id) {
    return auxiliary.findConditionById(id);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return auxiliary.findConditionsByParentJobId(parentJobId);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return auxiliary.findConditionsByChildJobId(childJobId);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return auxiliary.findConditionsByType(parentJobId, type);
  }

  @Override
  public void deleteConditionById(UUID id) {
    auxiliary.deleteConditionById(id);
  }

  @Override
  public void deleteConditionsByParentJobId(UUID parentJobId) {
    auxiliary.deleteConditionsByParentJobId(parentJobId);
  }

  @Override
  public void deleteConditionsByChildJobId(UUID childJobId) {
    auxiliary.deleteConditionsByChildJobId(childJobId);
  }

  @Override
  public long countConditionsByParentJobId(UUID parentJobId) {
    return auxiliary.countConditionsByParentJobId(parentJobId);
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    return batches.saveBatchMetrics(metrics);
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId) {
    return batches.findBatchMetrics(batchId);
  }

  @Override
  public void addChildExecutionTime(UUID batchId, long durationMs) {
    batches.addChildExecutionTime(batchId, durationMs);
  }

  @Override
  public void finalizeBatchMetrics(UUID batchId) {
    batches.finalizeBatchMetrics(batchId);
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    batches.updateBatchMetricsChildCount(batchId, childCount);
  }

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    return auxiliary.saveDlqAlert(alert);
  }

  @Override
  public boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff) {
    return auxiliary.existsRecentDlqAlert(jobId, errorHash, cutoff);
  }

  @Override
  public boolean tryAcquirePermit(String resource, UUID jobId, String nodeId) {
    return auxiliary.tryAcquirePermit(resource, jobId, nodeId);
  }

  @Override
  public void releasePermit(String resource, UUID jobId) {
    auxiliary.releasePermit(resource, jobId);
  }

  @Override
  public void releaseAllPermits(UUID jobId) {
    auxiliary.releaseAllPermits(jobId);
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    return auxiliary.getPermitRetryDelay(resource);
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    auxiliary.configureResource(name, maxConcurrent, retryDelayMs, description);
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    return auxiliary.cleanupOrphanedPermits(staleNodeIds);
  }

  @Override
  public List<JobEntity> searchJobs(JobFilter filter, int limit, int offset) {
    return query.searchJobs(filter, limit, offset);
  }

  @Override
  public long countJobs(JobFilter filter) {
    return query.countJobs(filter);
  }

  @Override
  public List<JobEntity> findTimedOutSignalJobs(Instant now, int limit) {
    return signals.findTimedOutSignalJobs(now, limit);
  }

  @Override
  public int deliverSignalById(
      UUID jobId,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    return signals.deliverSignalById(
        jobId,
        payload,
        payloadType,
        outcome,
        rejectionReason,
        deliveredBy,
        deliveredAt,
        deliveryId);
  }

  @Override
  public int deliverSignalByKey(
      String signalKey,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    return signals.deliverSignalByKey(
        signalKey,
        payload,
        payloadType,
        outcome,
        rejectionReason,
        deliveredBy,
        deliveredAt,
        deliveryId);
  }

  @Override
  public List<JobEntity> findJobsBySignalDeliveryId(String deliveryId) {
    return signals.findJobsBySignalDeliveryId(deliveryId);
  }

  @PostConstruct
  void initializeCollections() {
    validateUuidRepresentation();
    new MongoCollectionInitializer(database).initialize();
  }

  /**
   * Probes the codec registry by encoding a known UUID and inspecting the BSON binary subtype byte.
   * Subtype 4 (RFC 4122 / STANDARD) passes; any other subtype indicates a non-STANDARD {@code
   * UuidRepresentation} that would corrupt UUIDv7 round-trips and is rejected at startup.
   */
  private void validateUuidRepresentation() {
    Codec<UUID> codec = database.getCodecRegistry().get(UUID.class);
    byte[] bytes;
    try (BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      writer.writeStartDocument();
      writer.writeName("u");
      codec.encode(writer, new UUID(0L, 0L), EncoderContext.builder().build());
      writer.writeEndDocument();
      bytes = buffer.toByteArray();
    }
    // BSON layout for {"u": <binary>}:
    //   [0-3]  int32 totalSize
    //   [4]    0x05  (binary element type)
    //   [5]    'u'   (0x75)
    //   [6]    0x00  (cstring terminator)
    //   [7-10] int32 binary length (= 16)
    //   [11]   subtype  <-- byte we care about
    //   [12-27] 16 bytes of UUID data
    //   [28]   0x00  (document terminator)
    int subtype = bytes[11] & 0xFF;
    if (subtype != 4) {
      throw new RatchetConfigurationException(
          "ratchet-store-mongodb requires MongoClient with UuidRepresentation.STANDARD "
              + "(BSON binary subtype 4). Detected subtype "
              + subtype
              + " — likely UuidRepresentation.JAVA_LEGACY or another legacy variant. "
              + "Construct via MongoClientFactory.create(...) or set "
              + "MongoClientSettings.builder().uuidRepresentation(STANDARD) when supplying "
              + "your own MongoClient.");
    }
  }

  // ---------- RecurringJobStore delegates ----------

  @Override
  public List<run.ratchet.store.spi.RecurringJobDefinition> claimDueRecurring(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    return recurringJobs.claimDueRecurring(limit, nodeId, tagFilter);
  }

  @Override
  public void advanceNextFire(UUID id, Instant nextFire) {
    recurringJobs.advanceNextFire(id, nextFire);
  }

  @Override
  public void releaseClaim(UUID id) {
    recurringJobs.releaseClaim(id);
  }

  @Override
  public boolean cancelRecurringAndArchive(
      UUID id, run.ratchet.store.spi.RecurringJobStore.ArchiveReason reason) {
    return recurringJobs.cancelRecurringAndArchive(id, reason);
  }

  @Override
  public boolean cancelRecurringJobByBusinessKey(String businessKey) {
    return recurringJobs.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Override
  public UUID createRecurring(run.ratchet.store.spi.RecurringJobDefinition definition) {
    return recurringJobs.createRecurring(definition);
  }

  @Override
  public boolean updateRecurring(UUID id, run.ratchet.store.spi.RecurringJobDefinition definition) {
    return recurringJobs.updateRecurring(id, definition);
  }

  @Override
  public Optional<run.ratchet.store.spi.RecurringJobDefinition> getRecurring(UUID id) {
    return recurringJobs.getRecurring(id);
  }

  @Override
  public Optional<run.ratchet.store.spi.RecurringJobDefinition> findRecurringByBusinessKey(
      String businessKey) {
    return recurringJobs.findRecurringByBusinessKey(businessKey);
  }

  @Override
  public List<run.ratchet.store.spi.RecurringJobDefinition> listAll() {
    return recurringJobs.listAll();
  }
}
