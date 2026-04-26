package run.ratchet.testsuite.tck.clocked;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-threaded in-memory {@link run.ratchet.store.spi.JobStore} implementation that
 * supports just enough of the SPI to drive {@code AbstractDelayedSchedulingContract} green: submit
 * → claim (filtered by an injectable {@link Clock}) → mark-succeeded → workflow-noop.
 *
 * <p>All other SPI methods inherit {@link UnsupportedOperationException} stubs from {@link
 * ThrowingJobStoreBase}. If a future contract reaches a stubbed method, the failure surfaces with
 * the method name, making it obvious which SPI surface needs a real body.
 *
 * <p>The {@link Clock} dependency is what makes this store useful for the delayed-scheduling
 * contract: {@link #claimNextBatchOptimized} filters by {@code scheduledTime &lt;= clock.instant()}
 * rather than wall-clock {@code Instant.now()}, so a {@code SteppingTestClock} can
 * deterministically advance the eligibility horizon.
 */
@ApplicationScoped
@Alternative
@Priority(jakarta.interceptor.Interceptor.Priority.APPLICATION + 100)
public class InMemoryJobStore extends ThrowingJobStoreBase {

  private final Map<Long, JobEntity> jobs = new HashMap<>();
  private final Map<Long, List<JobExecutionEntity>> executions = new HashMap<>();
  private final AtomicLong idSeq = new AtomicLong(1);
  private final AtomicLong execIdSeq = new AtomicLong(1);
  private final Clock clock;

  protected InMemoryJobStore() {
    this.clock = Clock.systemUTC();
  }

  @Inject
  public InMemoryJobStore(Clock clock) {
    this.clock = clock;
  }

  /** Resets all stored state. Called from {@code RiClockedTckRuntime.clear()}. */
  public synchronized void reset() {
    jobs.clear();
    executions.clear();
  }

  // ----- JobCrudStore (real bodies) -----

  @Override
  public synchronized JobEntity save(JobEntity job) {
    if (job.getId() == null) {
      job.setId(idSeq.getAndIncrement());
    }
    if (job.getVersion() == null) {
      job.setVersion(0);
    }
    jobs.put(job.getId(), job);
    return job;
  }

  @Override
  public synchronized Optional<JobEntity> findById(long id) {
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public synchronized Optional<JobEntity> findByIdLatest(long id) {
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public synchronized Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    if (idempotencyKey == null) {
      return Optional.empty();
    }
    return jobs.values().stream()
        .filter(j -> idempotencyKey.equals(j.getIdempotencyKey()))
        .findFirst();
  }

  @Override
  public synchronized Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    if (businessKey == null) {
      return Optional.empty();
    }
    return jobs.values().stream()
        .filter(j -> businessKey.equals(j.getBusinessKey()))
        .filter(j -> j.getStatus() == JobStatus.PENDING || j.getStatus() == JobStatus.RUNNING)
        .findFirst();
  }

  @Override
  public synchronized List<JobEntity> findDependants(long parentJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized JobStatus getJobStatus(long id) {
    JobEntity job = jobs.get(id);
    return job == null ? null : job.getStatus();
  }

  // ----- JobClaimStore (real bodies) -----

  @Override
  public synchronized List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    var now = clock.instant();
    List<JobClaimDto> claimed = new ArrayList<>();
    for (JobEntity job : jobs.values()) {
      if (claimed.size() >= limit) {
        break;
      }
      if (job.getStatus() != JobStatus.PENDING) {
        continue;
      }
      if (job.getJobType() != jobType) {
        continue;
      }
      if (job.getScheduledTime() == null || job.getScheduledTime().isAfter(now)) {
        continue;
      }
      job.setStatus(JobStatus.RUNNING);
      job.setPickedBy(nodeId);
      job.setPickedAt(now);
      job.setVersion(job.getVersion() == null ? 1 : job.getVersion() + 1);
      claimed.add(
          new JobClaimDto(
              job.getId(),
              job.getStatus(),
              job.getJobType(),
              job.getPriority(),
              job.getScheduledTime(),
              job.getVersion(),
              job.getTimeoutSec(),
              job.getPickedBy(),
              job.getPickedAt(),
              job.getBusinessKey(),
              job.getAttempts(),
              job.getMaxRetries()));
    }
    return claimed;
  }

  @Override
  public synchronized List<JobEntity> claimNextBatch(int limit, String nodeId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    return Collections.emptyList();
  }

  // ----- JobTerminalStore (real bodies) -----

  @Override
  public synchronized boolean markJobSucceededMinimal(
      long id, java.time.Instant start, java.time.Instant end, Long durationMs, Long queueWaitMs) {
    JobEntity job = jobs.get(id);
    if (job == null) {
      return false;
    }
    job.setStatus(JobStatus.SUCCEEDED);
    job.setExecutionStartTime(start);
    job.setExecutionEndTime(end);
    job.setExecutionDurationMs(durationMs);
    job.setVersion(job.getVersion() == null ? 1 : job.getVersion() + 1);
    return true;
  }

  @Override
  public synchronized boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      java.time.Instant start,
      java.time.Instant end,
      Long durationMs,
      Long queueWaitMs) {
    return markJobSucceededMinimal(id, start, end, durationMs, queueWaitMs);
  }

  // ----- ExecutionStore (real bodies) -----

  @Override
  public synchronized JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      execution.setId(execIdSeq.getAndIncrement());
    }
    Long jobId = execution.getJobId();
    executions.computeIfAbsent(jobId, k -> new ArrayList<>()).add(execution);
    return execution;
  }

  @Override
  public synchronized Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    if (list == null || list.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(list.get(list.size() - 1));
  }

  @Override
  public synchronized List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    return list == null ? Collections.emptyList() : new ArrayList<>(list);
  }

  @Override
  public synchronized int countExecutionAttempts(long jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    return list == null ? 0 : list.size();
  }

  // ----- WorkflowConditionStore (real bodies — empty list is the contract for non-workflow jobs)
  // -----

  @Override
  public synchronized List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized long countConditionsByParentJobId(long parentJobId) {
    return 0L;
  }

  // ----- TagStore (no-op for tagless contract) -----

  @Override
  public synchronized void insertTags(long jobId, List<String> tags) {
    // No-op: AbstractDelayedSchedulingContract submits no tags.
  }

  // ----- NodeStore (no-op heartbeat infrastructure) -----

  @Override
  public synchronized void upsertHeartbeat(String nodeId, java.time.Instant ts) {
    // No-op: in-memory store has no node coordination needs.
  }

  @Override
  public synchronized Optional<run.ratchet.store.entity.NodeEntity> findNodeById(
      String nodeId) {
    return Optional.empty();
  }

  @Override
  public synchronized List<run.ratchet.store.entity.NodeEntity> findInactiveNodesSince(
      java.time.Instant cutoff) {
    return Collections.emptyList();
  }

  @Override
  public synchronized int deleteInactiveNodesSince(java.time.Instant cutoff) {
    return 0;
  }

  @Override
  public synchronized java.time.Instant getDatabaseTime() {
    return clock.instant();
  }

  // ----- JobBulkStore (no-op so background orphan-recovery loops don't trip stubs) -----

  @Override
  public synchronized int resetOrphanJobs(java.time.Duration grace) {
    return 0;
  }

  @Override
  public synchronized int resetOrphanJobsForNode(String nodeId) {
    return 0;
  }

  @Override
  public synchronized int deleteDlqOlderThan(java.time.Instant cutoff) {
    return 0;
  }

  // ----- LockStore (no-op leases — single-test, no contention) -----

  @Override
  public synchronized boolean tryLock(String name, java.time.Duration ttl, String nodeId) {
    return true;
  }

  @Override
  public synchronized void unlock(String name, String nodeId) {
    // No-op
  }

  @Override
  public synchronized boolean renewLock(String name, java.time.Duration extension, String nodeId) {
    return true;
  }

  // ----- JobBatchStatusStore (minimal status flips for resetRunningJobs at startup) -----

  @Override
  public synchronized int resetRunningJobs(String nodeId) {
    return 0;
  }

  // ----- JobCrudStore (counts that may be probed by metrics/observability beans) -----

  @Override
  public synchronized long countPendingJobs() {
    return 0L;
  }

  @Override
  public synchronized long countActiveNodes() {
    return 1L;
  }

  @Override
  public synchronized Optional<java.time.Instant> findEarliestRecurringNextFire() {
    return Optional.empty();
  }

  // ----- ResourcePermitStore (no-op so production permit-check is permissive) -----

  @Override
  public synchronized boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    return true;
  }

  @Override
  public synchronized void releasePermit(String resource, long jobId) {
    // No-op
  }

  @Override
  public synchronized void releaseAllPermits(long jobId) {
    // No-op
  }

  @Override
  public synchronized int cleanupOrphanedPermits(List<String> staleNodeIds) {
    return 0;
  }
}
