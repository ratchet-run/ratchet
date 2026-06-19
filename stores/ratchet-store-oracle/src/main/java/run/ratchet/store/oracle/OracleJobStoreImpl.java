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
package run.ratchet.store.oracle;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.MetricsCollector;
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
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.spi.RatchetEntityManagerProvider;
import run.ratchet.store.util.IsolationCheck;

/**
 * Package-private Oracle CDI implementation behind the public {@link OracleJobStore} type.
 *
 * <p>This class is intentionally a CDI/test wiring composite. Cohesive package-private operation
 * classes own the actual SQL for each store area, while this type owns injection, lifecycle, and
 * SPI forwarding.
 *
 * <p>The type-level {@link Transactional} annotation gives forwarded store writes the default
 * {@code REQUIRED} boundary. Read methods deliberately do NOT override with {@code SUPPORTS}: on
 * JTA-managed EclipseLink containers (Payara, GlassFish, OpenLiberty), calling a {@code SUPPORTS}
 * method outside an outer JTA tx leaks the borrowed pool connection with auto-commit disabled,
 * leaving an open InnoDB transaction holding metadata locks on subsequent writes (e.g. test-cleanup
 * truncates). The class-level {@code REQUIRED} keeps each read in a clean tx boundary that commits
 * before the connection returns to the pool.
 */
@ApplicationScoped
@Transactional
class OracleJobStoreImpl implements OracleJobStore {

  private final RatchetEntityManagerProvider entityManagerProvider;
  private final MetricsCollector metricsCollector;
  private final RatchetOptions options;
  private EntityManager em;

  private OracleJobCrudOperations jobs;
  private OracleJobQueryOperations query;
  private OracleJobClaimOperations claims;
  private OracleJobLifecycleOperations lifecycle;
  private OracleBatchOperations batches;
  private OracleNodeLockOperations nodeLocks;
  private OracleArchiveOperations archives;
  private OracleAuxiliaryOperations auxiliary;
  private OracleTagOperations tags;
  private OracleSignalOperations signals;
  private OracleRecurringJobOperations recurringJobs;

  /** No-arg constructor required by CDI normal-scope proxying. Not for direct use. */
  protected OracleJobStoreImpl() {
    this.entityManagerProvider = null;
    this.metricsCollector = null;
    this.options = null;
  }

  @Inject
  OracleJobStoreImpl(
      RatchetEntityManagerProvider entityManagerProvider,
      MetricsCollector metricsCollector,
      RatchetOptions options) {
    this.entityManagerProvider = entityManagerProvider;
    this.metricsCollector = metricsCollector;
    this.options = options;
  }

  @Override
  public JobEntity create(JobEntity job) {
    return jobs.create(job);
  }

  @Override
  public JobEntity save(JobEntity job) {
    return jobs.save(job);
  }

  @Override
  public Optional<JobEntity> findById(UUID id) {
    return jobs.findById(id);
  }

  @Override
  public Optional<JobEntity> findByIdLatest(UUID id) {
    return jobs.findByIdLatest(id);
  }

  @Override
  public void delete(UUID id) {
    jobs.delete(id);
  }

  @Override
  public JobStatus getJobStatus(UUID id) {
    return jobs.getJobStatus(id);
  }

  @Override
  public List<JobEntity> findByIds(List<UUID> ids) {
    return jobs.findByIds(ids);
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    return jobs.findActiveByBusinessKey(businessKey);
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    return jobs.findByIdempotencyKey(idempotencyKey);
  }

  @Override
  public List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    return jobs.findDependants(parentJobId, limit, offset);
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return recurringJobs.findEarliestRecurringNextFire();
  }

  @Override
  public long countPendingJobs() {
    return jobs.countPendingJobs();
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return jobs.countJobsByStatus(status);
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatuses() {
    return jobs.countJobsByStatuses();
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return jobs.countActiveJobs(jobType);
  }

  @Override
  public long countActiveNodes() {
    return jobs.countActiveNodes();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return jobs.countReadyJobs(now);
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return jobs.countStuckJobs(stuckThreshold);
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return jobs.countLongRunningJobs(threshold);
  }

  @Override
  public long countPendingBatchChildren() {
    return jobs.countPendingBatchChildren();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return jobs.countPendingJobsByPriority(priority);
  }

  @Override
  public Map<JobPriority, Long> countPendingJobsByPriorities() {
    return jobs.countPendingJobsByPriorities();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return jobs.countPendingJobsByType(jobType);
  }

  @Override
  public Map<JobExecutionType, Long> countPendingJobsByTypes() {
    return jobs.countPendingJobsByTypes();
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return jobs.countJobsByStatusSince(status, since);
  }

  @Override
  public long countJobsWithRetries() {
    return jobs.countJobsWithRetries();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    return jobs.getRetryRateStats(since);
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    return jobs.getAverageProcessingTime(since);
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    return jobs.getAverageBatchSize(since);
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    return jobs.getOldestPendingJobTime();
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    return jobs.getQueueWaitTimePercentile(percentile);
  }

  @Override
  public void bulkInsert(List<JobEntity> jobsToInsert) {
    jobs.bulkInsert(jobsToInsert);
  }

  @Override
  public int deleteJobsByIds(List<UUID> ids) {
    return jobs.deleteJobsByIds(ids);
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return jobs.deleteDlqOlderThan(cutoff);
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return jobs.resetOrphanJobs(grace);
  }

  @Override
  public int resetOrphanJobsBefore(Instant cutoff) {
    return jobs.resetOrphanJobsBefore(cutoff);
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return jobs.resetOrphanJobsForNode(nodeId);
  }

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    return claims.claimNextBatch(limit, nodeId, tagFilter);
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType,
      int limit,
      String nodeId,
      NodeTagFilter tagFilter,
      ExecutionTargetFilter executionTargetFilter) {
    return claims.claimNextBatchOptimized(jobType, limit, nodeId, tagFilter, executionTargetFilter);
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
  public boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return lifecycle.markJobFailedTerminal(id, terminalError, totalAttempts);
  }

  @Override
  public boolean cancelJob(UUID id) {
    return lifecycle.cancelJob(id);
  }

  @Override
  public boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    return lifecycle.scheduleJobRetry(id, error, newScheduledTime, attempts);
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return lifecycle.resetFailedToPending(id);
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
  public boolean pauseRecurring(UUID id) {
    return recurringJobs.pauseRecurring(id);
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    return recurringJobs.resumeRecurring(id);
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
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    return nodeLocks.tryLock(name, ttl, nodeId);
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void unlock(String name, String nodeId) {
    nodeLocks.unlock(name, nodeId);
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public boolean renewLock(String name, Duration extension, String nodeId) {
    return nodeLocks.renewLock(name, extension, nodeId);
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
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
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public int deleteInactiveNodesSince(Instant cutoff) {
    return nodeLocks.deleteInactiveNodesSince(cutoff);
  }

  @Override
  @Transactional(Transactional.TxType.REQUIRES_NEW)
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
  public int archiveJobsBatch(List<JobEntity> jobsToArchive, String reason, String archivedBy) {
    return archives.archiveJobsBatch(jobsToArchive, reason, archivedBy);
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
  public void appendLog(JobLogEntity log) {
    auxiliary.appendLog(log);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return auxiliary.purgeLogsOlderThan(cutoff);
  }

  @Override
  public void insertTags(UUID jobId, List<String> tagsToInsert) {
    tags.insertTags(jobId, tagsToInsert);
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
    return jobs.countJobsByStatusForTag(tag);
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return jobs.countJobsByParamForTag(tag, paramKey);
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return jobs.countJobsByExecutionNodeForTag(tag);
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
  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  void checkIsolationLevel() {
    if (em == null) {
      em = entityManagerProvider.getEntityManager();
    }
    IsolationCheck.verifyReadCommitted(
        em,
        "Oracle",
        List.of("SELECT @@SESSION.transaction_isolation", "SELECT @@SESSION.tx_isolation"),
        "READ-COMMITTED",
        "REPEATABLE READ causes InnoDB gap locks that block concurrent job enqueue during claim"
            + " queries. Set hibernate.connection.isolation=2 in persistence.xml or"
            + " transaction-isolation=TRANSACTION_READ_COMMITTED on the datasource.",
        options.store().isolationCheckMode());
    initDelegates();
  }

  private void initDelegates() {
    OracleStoreContext ctx =
        new OracleStoreContext(
            em, metricsCollector, options.store().priorityBoostIntervalMinutes());
    OracleJobRowMapper mapper = new OracleJobRowMapper();
    OracleBusinessKeyReservations reservations = new OracleBusinessKeyReservations(ctx);
    tags = new OracleTagOperations(ctx);
    OracleJobReadOperations reads = new OracleJobReadOperations(ctx, mapper, tags);
    jobs =
        new OracleJobCrudOperations(
            reads,
            new OracleJobCountOperations(ctx),
            new OracleJobDeleteOperations(ctx, reservations),
            new OracleJobWriteOperations(ctx, reservations, tags),
            tags);
    query = new OracleJobQueryOperations(ctx, mapper, tags);
    batches = new OracleBatchOperations(ctx);
    claims = new OracleJobClaimOperations(ctx, jobs);
    lifecycle = new OracleJobLifecycleOperations(ctx, reservations, batches);
    nodeLocks = new OracleNodeLockOperations(ctx);
    archives = new OracleArchiveOperations(ctx, mapper, tags, jobs);
    auxiliary = new OracleAuxiliaryOperations(ctx);
    signals = new OracleSignalOperations(ctx);
    recurringJobs = new OracleRecurringJobOperations(ctx, reservations);
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
  public boolean cancelRecurringAndArchive(
      UUID id, run.ratchet.store.spi.RecurringJobStore.ArchiveReason reason) {
    return recurringJobs.cancelRecurringAndArchive(id, reason);
  }

  // cancelOrphanedRecurringAnnotationJobs / cancelRecurringJobsByTag /
  // cancelRecurringJobsByBusinessKeys: identical signatures on JobBatchStatusStore and
  // RecurringJobStore; legacy delegates satisfy both.

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
