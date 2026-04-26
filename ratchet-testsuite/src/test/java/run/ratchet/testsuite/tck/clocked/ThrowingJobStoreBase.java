package run.ratchet.testsuite.tck.clocked;

import run.ratchet.api.JobPriority;
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
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Base implementation of the 18-interface {@link JobStore} marker that throws {@link
 * UnsupportedOperationException} from every method. Subclasses override only the operations they
 * need to support; everything else surfaces as a clear failure rather than a silent no-op.
 *
 * <p>Used by TCK runtimes that need a minimal, in-memory store for a narrow contract — e.g. {@code
 * InMemoryJobStore} for {@code AbstractDelayedSchedulingContract}.
 */
public abstract class ThrowingJobStoreBase implements JobStore {

  private static <T> T fail(String method) {
    throw new UnsupportedOperationException(
        "ThrowingJobStoreBase: "
            + method
            + " not implemented; this store is for narrow TCK use only");
  }

  // ----- JobCrudStore -----

  @Override
  public JobEntity save(JobEntity job) {
    return fail("save");
  }

  @Override
  public Optional<JobEntity> findById(long id) {
    return fail("findById");
  }

  @Override
  public Optional<JobEntity> findByIdLatest(long id) {
    return fail("findByIdLatest");
  }

  @Override
  public void delete(long id) {
    fail("delete");
  }

  @Override
  public JobStatus getJobStatus(long id) {
    return fail("getJobStatus");
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    return fail("findByIds");
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    return fail("findActiveByBusinessKey");
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    return fail("findByIdempotencyKey");
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    return fail("findDependants");
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return fail("findEarliestRecurringNextFire");
  }

  @Override
  public long countPendingJobs() {
    return fail("countPendingJobs");
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return fail("countJobsByStatus");
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return fail("countActiveJobs");
  }

  @Override
  public long countActiveNodes() {
    return fail("countActiveNodes");
  }

  @Override
  public long countReadyJobs(Instant now) {
    return fail("countReadyJobs");
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return fail("countStuckJobs");
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return fail("countLongRunningJobs");
  }

  @Override
  public long countPendingBatchChildren() {
    return fail("countPendingBatchChildren");
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return fail("countPendingJobsByPriority");
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return fail("countPendingJobsByType");
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return fail("countJobsByStatusSince");
  }

  @Override
  public long countJobsWithRetries() {
    return fail("countJobsWithRetries");
  }

  @Override
  public double getRetryRateStats(Instant since) {
    return fail("getRetryRateStats");
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    return fail("getAverageProcessingTime");
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    return fail("getAverageBatchSize");
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    return fail("getOldestPendingJobTime");
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    return fail("getQueueWaitTimePercentile");
  }

  // ----- JobClaimStore -----

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    return fail("claimNextBatch");
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    return fail("claimNextBatchOptimized");
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    return fail("claimDueRecurring");
  }

  // ----- JobTerminalStore -----

  @Override
  public boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    return fail("markJobSucceeded");
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return fail("markJobSucceededMinimal");
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
    return fail("markJobSucceededAndUpdateBatch");
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    return fail("markJobFailedTerminal");
  }

  @Override
  public boolean cancelJob(long id) {
    return fail("cancelJob");
  }

  // ----- JobRetryStore -----

  @Override
  public int incrementRetryAttempt(long id) {
    return fail("incrementRetryAttempt");
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    return fail("scheduleJobRetry");
  }

  @Override
  public boolean resetFailedToPending(long id) {
    return fail("resetFailedToPending");
  }

  // ----- JobPauseStore -----

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    return fail("transitionToPaused");
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    return fail("transitionFromPaused");
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    return fail("transitionFromPausedAtomic");
  }

  @Override
  public boolean pauseRecurring(long id) {
    return fail("pauseRecurring");
  }

  @Override
  public boolean resumeRecurring(long id) {
    return fail("resumeRecurring");
  }

  // ----- JobBatchStatusStore -----

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    fail("updateJobStatus");
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    return fail("compareAndSwapStatus");
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    return fail("tryPickUpJob");
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    return fail("resetRunningJob");
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return fail("resetRunningJobs");
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return fail("cancelRecurringJobsByTag");
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return fail("cancelRecurringJobByBusinessKey");
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return fail("cancelOrphanedRecurringAnnotationJobs");
  }

  // ----- JobBulkStore -----

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    fail("bulkInsert");
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    return fail("deleteJobsByIds");
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return fail("deleteDlqOlderThan");
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return fail("resetOrphanJobs");
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return fail("resetOrphanJobsForNode");
  }

  // ----- BatchStore -----

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    return fail("saveBatch");
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    return fail("findBatchById");
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    return fail("incrementCompletedAtomic");
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    return fail("incrementFailedAtomic");
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    return fail("markBatchCompleteIfReady");
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    return fail("findRecoverableBatchIds");
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    return fail("findBatchesByIds");
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    return fail("updateBatchTotalItems");
  }

  // ----- LockStore -----

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    return fail("tryLock");
  }

  @Override
  public void unlock(String name, String nodeId) {
    fail("unlock");
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    return fail("renewLock");
  }

  // ----- NodeStore -----

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    fail("upsertHeartbeat");
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return fail("findNodeById");
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    return fail("findInactiveNodesSince");
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return fail("deleteInactiveNodesSince");
  }

  @Override
  public Instant getDatabaseTime() {
    return fail("getDatabaseTime");
  }

  // ----- ArchiveStore -----

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    return fail("archiveJob");
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy) {
    return fail("archiveJobsBatch");
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    return fail("findJobsForArchiving");
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return fail("countJobsForArchiving");
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    return fail("findArchivedJobs");
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return fail("purgeArchivedJobs");
  }

  // ----- ExecutionStore -----

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    return fail("saveExecution");
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    return fail("findExecutionsByJobId");
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    return fail("findLatestExecution");
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return fail("countExecutionAttempts");
  }

  // ----- JobLogStore -----

  @Override
  public void appendLog(JobLogEntity log) {
    fail("appendLog");
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return fail("purgeLogsOlderThan");
  }

  // ----- TagStore -----

  @Override
  public void insertTags(long jobId, List<String> tags) {
    fail("insertTags");
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return fail("deleteTagsByJobId");
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    return fail("findJobIdsByTag");
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    return fail("countJobsByStatusForTag");
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return fail("countJobsByParamForTag");
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return fail("countJobsByExecutionNodeForTag");
  }

  // ----- WorkflowConditionStore -----

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    return fail("saveCondition");
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    return fail("findConditionById");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    return fail("findConditionsByParentJobId");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return fail("findConditionsByChildJobId");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    return fail("findConditionsByType");
  }

  @Override
  public void deleteConditionById(long id) {
    fail("deleteConditionById");
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    fail("deleteConditionsByParentJobId");
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    fail("deleteConditionsByChildJobId");
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return fail("countConditionsByParentJobId");
  }

  // ----- BatchMetricsStore -----

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    return fail("saveBatchMetrics");
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    return fail("findBatchMetrics");
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    fail("addChildExecutionTime");
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    fail("finalizeBatchMetrics");
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    fail("updateBatchMetricsChildCount");
  }

  // ----- DlqAlertStore -----

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    return fail("saveDlqAlert");
  }

  @Override
  public boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff) {
    return fail("existsRecentDlqAlert");
  }

  // ----- ResourcePermitStore -----

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    return fail("tryAcquirePermit");
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    fail("releasePermit");
  }

  @Override
  public void releaseAllPermits(long jobId) {
    fail("releaseAllPermits");
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    return fail("getPermitRetryDelay");
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    fail("configureResource");
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    return fail("cleanupOrphanedPermits");
  }
}
