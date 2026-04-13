package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStatusStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.logging.Logger;

/**
 * Priority-ordered retry buffers for jobs awaiting executor capacity. Separate bounded buffer per
 * {@link JobExecutionType}, drained by {@link RetryBufferDrainer}.
 */
@ApplicationScoped
@Transactional
public class RetryBufferManager {

  /** Normal capacity limit per job type; excess jobs are rejected by {@link #offer}. */
  static final int MAX_BUFFER_SIZE_PER_TYPE = 1000;

  /** Hard cap (2x normal) that even {@link #forceOffer} respects; breaches go to DLQ. */
  static final int HARD_CAP_PER_TYPE = 2000;

  private static final Logger log = Logger.getLogger(RetryBufferManager.class);

  private final DeadLetterService deadLetterService;
  private final JobStatusStore jobStatusStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  private final Map<JobExecutionType, Queue<BufferedJob>> retryBuffers =
      new EnumMap<>(JobExecutionType.class);

  private final Map<JobExecutionType, ReentrantLock> bufferLocks =
      new EnumMap<>(JobExecutionType.class);

  // Required by CDI proxy
  protected RetryBufferManager() {
    this.deadLetterService = null;
    this.jobStatusStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public RetryBufferManager(
      DeadLetterService deadLetterService,
      JobStatusStore jobStatusStore,
      NodeIdentityProvider nodeIdentityProvider) {
    this.deadLetterService = deadLetterService;
    this.jobStatusStore = jobStatusStore;
    this.nodeIdentityProvider = nodeIdentityProvider;

    Comparator<BufferedJob> jobComparator =
        Comparator.comparing(
                (BufferedJob job) -> job.priority().ordinal(), Comparator.reverseOrder())
            .thenComparing(BufferedJob::scheduledTime);

    for (JobExecutionType jobType : JobExecutionType.values()) {
      retryBuffers.put(jobType, new PriorityBlockingQueue<>(100, jobComparator));
      bufferLocks.put(jobType, new ReentrantLock());
    }
  }

  /**
   * Adds a job to its retry buffer, bypassing the normal size limit but respecting the hard cap.
   *
   * <p>Use this method only when the job must be buffered regardless of normal capacity limits,
   * such as during recovery operations when resetting to PENDING status fails. In normal operation,
   * prefer {@link #offer(JobEntity)} to respect backpressure limits.
   *
   * <p><b>Safety mechanism:</b> While this method bypasses {@link #MAX_BUFFER_SIZE_PER_TYPE}, it
   * still respects {@link #HARD_CAP_PER_TYPE} to prevent unbounded memory growth during
   * catastrophic failure scenarios. Jobs that exceed the hard cap are dropped with an error log.
   *
   * @param job the job to buffer for later retry
   * @return true if the job was buffered, false if the hard cap was reached and the job was dropped
   */
  public boolean forceOffer(JobEntity job) {
    Queue<BufferedJob> buffer = retryBuffers.get(job.getJobType());
    ReentrantLock lock = bufferLocks.get(job.getJobType());

    lock.lock();
    try {
      // Enforce hard cap even for forced offers to prevent memory exhaustion
      if (buffer.size() >= HARD_CAP_PER_TYPE) {
        log.error(
            String.format(
                "CRITICAL: Retry buffer hard cap (%d) reached for job type %s. "
                    + "Job %s moving to DLQ to prevent loss. "
                    + "This indicates sustained system failure - investigate immediately.",
                HARD_CAP_PER_TYPE, job.getJobType(), job.getId()));
        // Move job to DLQ instead of dropping it silently
        deadLetterService.moveToDlq(
            job,
            new IllegalStateException(
                "Retry buffer hard cap exceeded for job type " + job.getJobType()));
        return false;
      }

      // Log warning when exceeding normal limit (but under hard cap)
      if (buffer.size() >= MAX_BUFFER_SIZE_PER_TYPE) {
        log.warn(
            String.format(
                "Retry buffer exceeding normal limit (%d) for job type %s. "
                    + "Current size: %d. Force-buffering job %s.",
                MAX_BUFFER_SIZE_PER_TYPE, job.getJobType(), buffer.size(), job.getId()));
      }

      return buffer.offer(BufferedJob.from(job));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns an unmodifiable view of the retry buffer for a specific job type.
   *
   * <p>The returned collection is a read-only snapshot view. To poll or modify the buffer, use
   * {@link #pollFromBuffer(JobExecutionType)} or {@link #pollBatchFromBuffer(JobExecutionType,
   * int)} which enforce the required locking protocol.
   *
   * @param jobType the job type to get the buffer for
   * @return an unmodifiable view of the priority queue for the specified job type
   */
  public Collection<BufferedJob> getBuffer(JobExecutionType jobType) {
    Queue<BufferedJob> queue = retryBuffers.get(jobType);
    return queue != null ? Collections.unmodifiableCollection(queue) : List.of();
  }

  /**
   * Thread-safe poll from the retry buffer for a specific job type.
   *
   * <p>This method synchronizes on the buffer to ensure safe access from concurrent drain
   * operations. Prefer this over {@code getBuffer(jobType).poll()} in production code paths.
   *
   * @param jobType the job type to poll from
   * @return the highest-priority buffered job, or null if the buffer is empty
   */
  public BufferedJob pollFromBuffer(JobExecutionType jobType) {
    Queue<BufferedJob> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      return buffer.poll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Polls up to {@code limit} buffered jobs for a single job type while preserving priority order.
   *
   * @param jobType the job type to drain
   * @param limit maximum number of entries to remove
   * @return buffered jobs in poll order
   */
  public List<BufferedJob> pollBatchFromBuffer(JobExecutionType jobType, int limit) {
    Queue<BufferedJob> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      List<BufferedJob> jobs = new ArrayList<>(Math.max(limit, 0));
      for (int i = 0; i < limit; i++) {
        BufferedJob buffered = buffer.poll();
        if (buffered == null) {
          break;
        }
        jobs.add(buffered);
      }
      return jobs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns whether the retry buffer for a specific job type is empty.
   *
   * @param jobType the job type to check
   * @return true if the buffer is empty
   */
  public boolean isBufferEmpty(JobExecutionType jobType) {
    Queue<BufferedJob> buffer = retryBuffers.get(jobType);
    ReentrantLock lock = bufferLocks.get(jobType);
    lock.lock();
    try {
      return buffer.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Attempts to add a job to its retry buffer, respecting the size limit.
   *
   * <p>If the buffer for the job's type is at capacity, this method returns false and the job is
   * not added. The caller must handle this case appropriately (e.g., by failing the job or
   * returning it to PENDING in the database).
   *
   * @param job the job to buffer for later retry
   * @return true if the job was added, false if the buffer is at capacity
   */
  public boolean offer(JobEntity job) {
    Queue<BufferedJob> buffer = retryBuffers.get(job.getJobType());
    ReentrantLock lock = bufferLocks.get(job.getJobType());
    lock.lock();
    try {
      if (buffer.size() >= MAX_BUFFER_SIZE_PER_TYPE) {
        return false;
      }
      return buffer.offer(BufferedJob.from(job));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the total number of jobs across all retry buffers.
   *
   * <p>This metric indicates overall backpressure in the system. A consistently high value suggests
   * thread pools are undersized or job execution is too slow.
   *
   * @return total count of buffered jobs across all job types
   */
  public int totalSize() {
    int total = 0;
    for (Map.Entry<JobExecutionType, Queue<BufferedJob>> entry : retryBuffers.entrySet()) {
      ReentrantLock lock = bufferLocks.get(entry.getKey());
      lock.lock();
      try {
        Queue<BufferedJob> buffer = entry.getValue();
        total += buffer.size();
      } finally {
        lock.unlock();
      }
    }
    return total;
  }

  /**
   * Flushes all buffered jobs back to PENDING status in the database on shutdown.
   *
   * <p>This prevents job loss when the application shuts down while jobs are waiting in the retry
   * buffer. Each buffered job is reset to PENDING so it can be picked up by another node or on
   * restart.
   */
  public void flushOnShutdown() {
    int flushed = 0;
    String nodeId = nodeIdentityProvider.getNodeId();
    for (Map.Entry<JobExecutionType, Queue<BufferedJob>> entry : retryBuffers.entrySet()) {
      Queue<BufferedJob> buffer = entry.getValue();
      ReentrantLock lock = bufferLocks.get(entry.getKey());
      lock.lock();
      try {
        BufferedJob buffered;
        while ((buffered = buffer.poll()) != null) {
          try {
            if (jobStatusStore.resetRunningJob(buffered.jobId(), nodeId)) {
              flushed++;
            }
          } catch (Exception e) {
            log.errorf(
                e, "Failed to reset buffered job %s to PENDING on shutdown", buffered.jobId());
          }
        }
      } finally {
        lock.unlock();
      }
    }
    if (flushed > 0) {
      log.infof("RetryBufferManager shutdown: flushed %s buffered job(s) back to PENDING", flushed);
    }
  }

  /**
   * Lightweight DTO for buffered jobs to avoid holding full entity references in memory. The full
   * entity is loaded from the database only when the job is drained for resubmission.
   */
  public record BufferedJob(
      Long jobId, JobExecutionType jobType, JobPriority priority, Instant scheduledTime) {

    /** Creates a BufferedJob from a full JobEntity. */
    static BufferedJob from(JobEntity job) {
      return new BufferedJob(
          job.getId(), job.getJobType(), job.getPriority(), job.getScheduledTime());
    }
  }
}
