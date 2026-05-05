package run.ratchet.testsuite.tck.clocked;

import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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

  private final Map<UUID, JobEntity> jobs = new HashMap<>();
  private final Map<UUID, List<JobExecutionEntity>> executions = new HashMap<>();
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
  public synchronized JobEntity create(JobEntity job) {
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
    if (job.getVersion() == null) {
      job.setVersion(0);
    }
    jobs.put(job.getId(), job);
    return job;
  }

  @Override
  public synchronized JobEntity save(JobEntity job) {
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
    if (job.getVersion() == null) {
      job.setVersion(0);
    }
    jobs.put(job.getId(), job);
    return job;
  }

  @Override
  public synchronized Optional<JobEntity> findById(UUID id) {
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public synchronized Optional<JobEntity> findByIdLatest(UUID id) {
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
        .filter(
            j ->
                j.getStatus() == JobStatus.PENDING
                    || j.getStatus() == JobStatus.RUNNING
                    || j.getStatus() == JobStatus.WAITING)
        .findFirst();
  }

  @Override
  public synchronized List<JobEntity> findDependants(UUID parentJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized JobStatus getJobStatus(UUID id) {
    JobEntity job = jobs.get(id);
    return job == null ? null : job.getStatus();
  }

  @Override
  public synchronized boolean cancelJob(UUID id) {
    JobEntity job = jobs.get(id);
    if (job == null) {
      return false;
    }
    JobStatus status = job.getStatus();
    if (status != JobStatus.PENDING && status != JobStatus.RUNNING && status != JobStatus.WAITING) {
      return false;
    }
    job.setStatus(JobStatus.CANCELED);
    job.setVersion(job.getVersion() == null ? 1 : job.getVersion() + 1);
    return true;
  }

  @Override
  public synchronized boolean compareAndSwapStatus(
      UUID id, JobStatus expected, JobStatus newStatus, String error) {
    JobEntity job = jobs.get(id);
    if (job == null || job.getStatus() != expected) {
      return false;
    }
    job.setStatus(newStatus);
    if (error != null) {
      job.setLastError(error);
    }
    job.setVersion(job.getVersion() == null ? 1 : job.getVersion() + 1);
    return true;
  }

  // ----- JobClaimStore (real bodies) -----

  @Override
  public synchronized List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter) {
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
      if (!matchesTagFilter(job, tagFilter)) {
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
  public synchronized List<JobEntity> claimNextBatch(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    return Collections.emptyList();
  }

  @Override
  public synchronized List<JobEntity> claimDueRecurring(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    return Collections.emptyList();
  }

  private static boolean matchesTagFilter(JobEntity job, NodeTagFilter filter) {
    if (filter.isUnfiltered()) {
      return true;
    }
    List<String> tags = job.getTags() == null ? List.of() : job.getTags();
    if (!filter.requireTags().isEmpty()) {
      boolean hasRequired = tags.stream().anyMatch(filter.requireTags()::contains);
      if (!hasRequired) {
        return false;
      }
    }
    if (!filter.excludeTags().isEmpty()) {
      boolean hasExcluded = tags.stream().anyMatch(filter.excludeTags()::contains);
      if (hasExcluded) {
        return false;
      }
    }
    return true;
  }

  // ----- JobTerminalStore (real bodies) -----

  @Override
  public synchronized boolean markJobSucceededMinimal(
      UUID id, java.time.Instant start, java.time.Instant end, Long durationMs, Long queueWaitMs) {
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
      UUID id,
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
      execution.setId(UuidV7Factory.create());
    }
    UUID jobId = execution.getJobId();
    executions.computeIfAbsent(jobId, k -> new ArrayList<>()).add(execution);
    return execution;
  }

  @Override
  public synchronized Optional<JobExecutionEntity> findLatestExecution(UUID jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    if (list == null || list.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(list.get(list.size() - 1));
  }

  @Override
  public synchronized List<JobExecutionEntity> findExecutionsByJobId(UUID jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    return list == null ? Collections.emptyList() : new ArrayList<>(list);
  }

  @Override
  public synchronized int countExecutionAttempts(UUID jobId) {
    List<JobExecutionEntity> list = executions.get(jobId);
    return list == null ? 0 : list.size();
  }

  // ----- SignalStore (real bodies for signal-aware API contracts) -----

  @Override
  public synchronized List<JobEntity> findTimedOutSignalJobs(Instant now) {
    return jobs.values().stream()
        .filter(j -> j.getStatus() == JobStatus.WAITING)
        .filter(j -> j.getSignalTimeout() != null && !j.getSignalTimeout().isAfter(now))
        .toList();
  }

  @Override
  public synchronized int deliverSignalById(
      UUID jobId,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    JobEntity job = jobs.get(jobId);
    if (job == null || job.getStatus() != JobStatus.WAITING) {
      return 0;
    }
    applySignalDelivery(
        job, payload, payloadType, outcome, rejectionReason, deliveredBy, deliveredAt, deliveryId);
    return 1;
  }

  @Override
  public synchronized int deliverSignalByKey(
      String signalKey,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    int delivered = 0;
    for (JobEntity job : jobs.values()) {
      if (job.getStatus() == JobStatus.WAITING && signalKey.equals(job.getSignalKey())) {
        applySignalDelivery(
            job,
            payload,
            payloadType,
            outcome,
            rejectionReason,
            deliveredBy,
            deliveredAt,
            deliveryId);
        delivered++;
      }
    }
    return delivered;
  }

  @Override
  public synchronized List<JobEntity> findJobsBySignalDeliveryId(String deliveryId) {
    return jobs.values().stream()
        .filter(j -> deliveryId != null && deliveryId.equals(j.getSignalDeliveryId()))
        .toList();
  }

  private static void applySignalDelivery(
      JobEntity job,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    job.setStatus(JobStatus.PENDING);
    job.setSignalPayload(payload);
    job.setSignalPayloadType(payloadType);
    job.setSignalOutcome(outcome);
    job.setSignalRejectionReason(rejectionReason);
    job.setSignalDeliveredBy(deliveredBy);
    job.setSignalDeliveredAt(deliveredAt);
    job.setSignalDeliveryId(deliveryId);
    job.setVersion(job.getVersion() == null ? 1 : job.getVersion() + 1);
  }

  // ----- WorkflowConditionStore (real bodies — empty list is the contract for non-workflow jobs)
  // -----

  @Override
  public synchronized List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return Collections.emptyList();
  }

  @Override
  public synchronized long countConditionsByParentJobId(UUID parentJobId) {
    return 0L;
  }

  // ----- TagStore (no-op for tagless contract) -----

  @Override
  public synchronized void insertTags(UUID jobId, List<String> tags) {
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
  public synchronized boolean tryAcquirePermit(String resource, UUID jobId, String nodeId) {
    return true;
  }

  @Override
  public synchronized void releasePermit(String resource, UUID jobId) {
    // No-op
  }

  @Override
  public synchronized void releaseAllPermits(UUID jobId) {
    // No-op
  }

  @Override
  public synchronized int cleanupOrphanedPermits(List<String> staleNodeIds) {
    return 0;
  }
}
