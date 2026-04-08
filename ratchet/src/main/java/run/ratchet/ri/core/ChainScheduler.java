package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Manages the execution flow and lifecycle of chained jobs within the scheduler framework. This
 * service handles job dependencies by orchestrating the sequential execution of jobs that form a
 * chain, where each job's execution depends on the successful completion of its predecessor.
 *
 * <p>The ChainScheduler implements two key behaviors:
 *
 * <ul>
 *   <li><b>Success propagation:</b> When a job completes successfully, it automatically schedules
 *       its immediate dependent jobs for execution
 *   <li><b>Failure cascading:</b> When a job fails permanently, it recursively cancels all
 *       downstream dependent jobs to prevent execution of invalid workflows
 * </ul>
 *
 * <p>The scheduler uses a depth-first traversal algorithm to process dependency trees, ensuring all
 * transitive dependencies are properly handled during cancellation.
 *
 * @see JobEntity#getDependsOn() for dependency configuration
 * @see JobStatus for job state transitions
 */
@ApplicationScoped
@Transactional
public class ChainScheduler {

  /**
   * Sentinel timestamp used to lock chain jobs until their predecessors complete. Jobs with this
   * scheduled time are invisible to the poller.
   */
  static final Instant CHAIN_LOCK_TIME = Instant.parse("9999-12-31T23:59:59Z");

  private static final Logger log = Logger.getLogger(ChainScheduler.class);

  /** Store for job entity operations. */
  private final JobCrudStore jobCrudStore;

  // Required by CDI proxy
  protected ChainScheduler() {
    this.jobCrudStore = null;
  }

  @Inject
  public ChainScheduler(JobCrudStore jobCrudStore) {
    this.jobCrudStore = jobCrudStore;
  }

  /**
   * Cancels a chain of dependent jobs when a specified job fails permanently.
   *
   * <p>This method implements failure cascading for job chains by recursively traversing the
   * dependency tree and canceling all downstream jobs. The traversal uses a depth-first algorithm
   * with an explicit stack to avoid stack overflow on deep chains.
   *
   * <p>Only jobs in PENDING status are canceled; jobs that have already started execution
   * (PROCESSING) or completed (SUCCEEDED, FAILED) are left unchanged.
   *
   * @param failed the {@link JobEntity} representing the failed job whose dependents should be
   *     canceled; must not be null
   */
  public void cancelChain(JobEntity failed) {
    Deque<Long> stack = new ArrayDeque<>();
    stack.push(failed.getId());

    while (!stack.isEmpty()) {
      long parentId = stack.pop();
      List<JobEntity> children = jobCrudStore.findDependants(parentId);
      for (JobEntity child : children) {
        if (child.getStatus() == JobStatus.PENDING) {
          child.setStatus(JobStatus.CANCELED);
          jobCrudStore.save(child);
          log.warnf("Chain step %s canceled (ancestor failed %s)", child.getId(), failed.getId());
        }
        stack.push(child.getId());
      }
    }
  }

  /**
   * Schedules the next dependent jobs in a chain after successful completion of a predecessor.
   *
   * <p>This method implements success propagation for job chains by finding all jobs that depend on
   * the finished job and making them eligible for execution. Only immediate dependents are
   * unlocked; transitive dependencies remain locked until their direct predecessors complete.
   *
   * <p>A job is unlocked by updating its {@code scheduledTime} from the special {@link
   * #CHAIN_LOCK_TIME} sentinel value to the current time, making it visible to the poller for
   * execution.
   *
   * @param finished the {@link JobEntity} representing the successfully completed job; used to find
   *     dependent jobs that should now be scheduled
   */
  public void scheduleNext(JobEntity finished) {
    List<JobEntity> children = jobCrudStore.findDependants(finished.getId());
    if (children.isEmpty()) {
      return;
    }

    for (JobEntity c : children) {
      if (c.getStatus() == JobStatus.PENDING && CHAIN_LOCK_TIME.equals(c.getScheduledTime())) {
        c.setScheduledTime(Instant.now());
        jobCrudStore.save(c);
        log.infof("Chain step %s unlocked (prev=%s)", c.getId(), finished.getId());
      }
    }
  }
}
