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

/** Propagates success/failure through chained job dependencies. */
@ApplicationScoped
@Transactional
public class ChainScheduler {

  /**
   * Sentinel timestamp that keeps chain jobs invisible to the poller until predecessors complete.
   */
  static final Instant CHAIN_LOCK_TIME = Instant.parse("9999-12-31T23:59:59Z");

  private static final Logger log = Logger.getLogger(ChainScheduler.class);

  private final JobCrudStore jobCrudStore;

  protected ChainScheduler() {
    this.jobCrudStore = null;
  }

  @Inject
  public ChainScheduler(JobCrudStore jobCrudStore) {
    this.jobCrudStore = jobCrudStore;
  }

  /** Cancels all downstream dependents of a failed job using depth-first traversal. */
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

  /** Unlocks immediate dependents of a completed job by resetting their scheduled time. */
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
