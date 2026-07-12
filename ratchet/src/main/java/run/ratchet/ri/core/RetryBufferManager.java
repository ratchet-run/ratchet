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
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;

/**
 * Priority-ordered retry buffers for claimed jobs awaiting executor capacity. Separate bounded
 * buffer per {@link JobExecutionType}, drained by {@link RetryBufferDrainer}.
 */
@ApplicationScoped
class RetryBufferManager {

  static final int MAX_BUFFER_SIZE_PER_TYPE = 1000;

  static final int HARD_CAP_PER_TYPE = 2000;

  private static final Logger log = Logger.getLogger(RetryBufferManager.class);

  private final DeadLetterService deadLetterService;
  private final JobStateManager jobStateManager;

  private final Map<JobExecutionType, Queue<BufferedClaim>> retryBuffers =
      new EnumMap<>(JobExecutionType.class);

  private final Map<JobExecutionType, ReentrantLock> bufferLocks =
      new EnumMap<>(JobExecutionType.class);

  protected RetryBufferManager() {
    this.deadLetterService = null;
    this.jobStateManager = null;
  }

  @Inject
  public RetryBufferManager(DeadLetterService deadLetterService, JobStateManager jobStateManager) {
    this.deadLetterService = deadLetterService;
    this.jobStateManager = jobStateManager;

    Comparator<BufferedClaim> jobComparator =
        Comparator.comparing(
                (BufferedClaim job) -> job.priority().persistedCode(), Comparator.reverseOrder())
            .thenComparing(BufferedClaim::scheduledTime);

    for (JobExecutionType jobType : JobExecutionType.values()) {
      retryBuffers.put(jobType, new PriorityBlockingQueue<>(100, jobComparator));
      bufferLocks.put(jobType, new ReentrantLock());
    }
  }

  private static JobEntity toDlqJob(BufferedClaim claim) {
    JobEntity job = new JobEntity();
    job.setId(claim.jobId());
    job.setJobType(claim.jobType());
    job.setPriority(claim.priority());
    job.setScheduledTime(claim.scheduledTime());
    job.setTimeoutSec(claim.timeoutSec());
    job.setPickedBy(claim.pickedBy());
    job.setPickedAt(claim.pickedAt());
    job.setBusinessKey(claim.businessKey());
    job.setAttempts(claim.attempts());
    job.setMaxRetries(claim.maxRetries());
    job.setStatus(JobStatus.RUNNING);
    return job;
  }

  /**
   * Buffers a claim, bypassing {@link #MAX_BUFFER_SIZE_PER_TYPE} but still respecting {@link
   * #HARD_CAP_PER_TYPE}. Claims exceeding the hard cap are moved to the DLQ to avoid silent loss.
   *
   * @return true if buffered, false if the hard cap was reached
   */
  public boolean forceOffer(JobEntity job) {
    return forceOffer(BufferedClaim.from(job));
  }

  public boolean forceOffer(JobClaimDto claim) {
    return forceOffer(BufferedClaim.from(claim));
  }

  /**
   * Stable unmodifiable snapshot; use {@link #pollFromBuffer} or {@link #pollBatchFromBuffer} to
   * drain.
   */
  public Collection<BufferedClaim> getBuffer(JobExecutionType jobType) {
    Queue<BufferedClaim> queue = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    if (queue == null || lock == null) {
      return List.of();
    }
    lock.lock();
    try {
      return Collections.unmodifiableList(new ArrayList<>(queue));
    } finally {
      lock.unlock();
    }
  }

  public BufferedClaim pollFromBuffer(JobExecutionType jobType) {
    Queue<BufferedClaim> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      return buffer.poll();
    } finally {
      lock.unlock();
    }
  }

  public List<BufferedClaim> pollBatchFromBuffer(JobExecutionType jobType, int limit) {
    return pollBatchFromBuffer(jobType, ExecutionTargetFilter.any(), limit);
  }

  public List<BufferedClaim> pollBatchFromBuffer(
      JobExecutionType jobType, ExecutionTargetFilter executionTargetFilter, int limit) {
    Queue<BufferedClaim> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      List<BufferedClaim> jobs = new ArrayList<>(Math.max(limit, 0));
      List<BufferedClaim> skipped = new ArrayList<>();
      for (int i = 0; i < limit; i++) {
        BufferedClaim buffered = buffer.poll();
        if (buffered == null) {
          break;
        }
        if (executionTargetFilter == null
            || executionTargetFilter.matches(buffered.executionTarget())) {
          jobs.add(buffered);
        } else {
          skipped.add(buffered);
          i--;
        }
      }
      buffer.addAll(skipped);
      return jobs;
    } finally {
      lock.unlock();
    }
  }

  public boolean isBufferEmpty(JobExecutionType jobType) {
    Queue<BufferedClaim> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      return buffer.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return true if buffered, false if the buffer is at capacity ({@link
   *     #MAX_BUFFER_SIZE_PER_TYPE})
   */
  public boolean offer(JobEntity job) {
    return offer(BufferedClaim.from(job));
  }

  public boolean offer(JobClaimDto claim) {
    return offer(BufferedClaim.from(claim));
  }

  public int totalSize() {
    int total = 0;
    for (Map.Entry<JobExecutionType, Queue<BufferedClaim>> entry : retryBuffers.entrySet()) {
      ReentrantLock lock = bufferLocks.get(entry.getKey());
      lock.lock();
      try {
        total += entry.getValue().size();
      } finally {
        lock.unlock();
      }
    }
    return total;
  }

  /**
   * Flushes every buffered claim back to PENDING so jobs this node holds only in memory can be
   * picked up elsewhere after shutdown.
   *
   * <p>Each claim is reset in its own transaction by delegating to {@link
   * JobStateManager#resetJobToPending(java.util.UUID)} (transaction attribute REQUIRED, invoked
   * across a bean boundary so a new transaction begins per claim). This method is deliberately not
   * {@code @Transactional}: a single enclosing transaction would let one failed reset mark the
   * whole batch rollback-only and silently undo every claim already flushed.
   *
   * <p>Failed resets are requeued and counted. The method never throws after a partial flush, since
   * doing so would discard claims it already moved back to PENDING.
   */
  public void flushOnShutdown() {
    int flushed = 0;
    int failedCount = 0;
    for (Map.Entry<JobExecutionType, Queue<BufferedClaim>> entry : retryBuffers.entrySet()) {
      Queue<BufferedClaim> buffer = entry.getValue();
      ReentrantLock lock = bufferLocks.get(entry.getKey());
      lock.lock();
      try {
        List<BufferedClaim> failedForType = new ArrayList<>();
        BufferedClaim buffered;
        while ((buffered = buffer.poll()) != null) {
          try {
            if (jobStateManager.resetJobToPending(buffered.jobId())) {
              flushed++;
            }
          } catch (Exception e) {
            log.errorf(e, "Buffer reset error for job %s on shutdown", buffered.jobId());
            failedForType.add(buffered);
          }
        }
        buffer.addAll(failedForType);
        failedCount += failedForType.size();
      } finally {
        lock.unlock();
      }
    }
    if (flushed > 0) {
      log.infof("RetryBufferManager shutdown: flushed %s buffered job(s) back to PENDING", flushed);
    }
    if (failedCount > 0) {
      log.warnf(
          "RetryBufferManager shutdown: %s buffered job(s) could not be reset and will rely on"
              + " orphan recovery",
          failedCount);
    }
  }

  private boolean forceOffer(BufferedClaim claim) {
    Queue<BufferedClaim> buffer = retryBuffers.get(claim.jobType());
    ReentrantLock lock = bufferLocks.get(claim.jobType());

    lock.lock();
    try {
      if (buffer.size() < HARD_CAP_PER_TYPE) {
        if (buffer.size() >= MAX_BUFFER_SIZE_PER_TYPE) {
          log.warnf(
              "Retry buffer exceeding normal limit (%d) for job type %s. "
                  + "Current size: %d. Force-buffering job %s.",
              MAX_BUFFER_SIZE_PER_TYPE, claim.jobType(), buffer.size(), claim.jobId());
        }
        return buffer.offer(claim);
      }
    } finally {
      lock.unlock();
    }
    return moveHardCapOverflow(claim, buffer, lock);
  }

  private boolean moveHardCapOverflow(
      BufferedClaim claim, Queue<BufferedClaim> buffer, ReentrantLock lock) {
    log.errorf(
        "CRITICAL: Retry buffer hard cap (%d) reached for job type %s. "
            + "Job %s moving to DLQ to prevent loss. "
            + "This indicates sustained system failure - investigate immediately.",
        HARD_CAP_PER_TYPE, claim.jobType(), claim.jobId());
    try {
      deadLetterService.moveToDlq(
          toDlqJob(claim),
          new IllegalStateException(
              "Retry buffer hard cap exceeded for job type " + claim.jobType()));
      return false;
    } catch (Exception e) {
      log.errorf(
          e,
          "CRITICAL: Failed to move job %s to DLQ after retry buffer hard cap. "
              + "Force-buffering beyond hard cap to avoid losing the claimed job.",
          claim.jobId());
      lock.lock();
      try {
        return buffer.offer(claim);
      } finally {
        lock.unlock();
      }
    }
  }

  private boolean offer(BufferedClaim claim) {
    Queue<BufferedClaim> buffer = retryBuffers.get(claim.jobType());
    ReentrantLock lock = bufferLocks.get(claim.jobType());
    lock.lock();
    try {
      if (buffer.size() >= MAX_BUFFER_SIZE_PER_TYPE) {
        return false;
      }
      return buffer.offer(claim);
    } finally {
      lock.unlock();
    }
  }

  public record BufferedClaim(
      UUID jobId,
      JobExecutionType jobType,
      JobPriority priority,
      Instant scheduledTime,
      int timeoutSec,
      String pickedBy,
      Instant pickedAt,
      String businessKey,
      int attempts,
      int maxRetries,
      String executionTarget) {

    static BufferedClaim from(JobEntity job) {
      return new BufferedClaim(
          job.getId(),
          job.getJobType(),
          job.getPriority(),
          job.getScheduledTime(),
          job.getTimeoutSec(),
          job.getPickedBy(),
          job.getPickedAt(),
          job.getBusinessKey(),
          job.getAttempts(),
          job.getMaxRetries(),
          job.getExecutionTarget());
    }

    static BufferedClaim from(JobClaimDto claim) {
      return new BufferedClaim(
          claim.id(),
          claim.jobType(),
          claim.priority(),
          claim.scheduledTime(),
          claim.timeoutSec(),
          claim.pickedBy(),
          claim.pickedAt(),
          claim.businessKey(),
          claim.attempts(),
          claim.maxRetries(),
          claim.executionTarget());
    }

    JobClaimDto toClaimDto() {
      return new JobClaimDto(
          jobId,
          JobStatus.RUNNING,
          jobType,
          priority,
          scheduledTime,
          0,
          timeoutSec,
          pickedBy,
          pickedAt,
          businessKey,
          attempts,
          maxRetries,
          executionTarget);
    }
  }
}
