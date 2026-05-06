package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;

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

  public void cancelChain(JobEntity failed) {
    Deque<UUID> stack = new ArrayDeque<>();
    stack.push(failed.getId());

    while (!stack.isEmpty()) {
      UUID parentId = stack.pop();
      List<JobEntity> children = jobCrudStore.findDependants(parentId);
      for (JobEntity child : children) {
        JobStatus status = child.getStatus();
        if (status == JobStatus.PENDING || status == JobStatus.WAITING) {
          child.setStatus(JobStatus.CANCELED);
          jobCrudStore.save(child);
          log.warnf("Chain step %s canceled (ancestor failed %s)", child.getId(), failed.getId());
        }
        stack.push(child.getId());
      }
    }
  }

  public boolean scheduleNext(JobEntity finished) {
    List<JobEntity> children = jobCrudStore.findDependants(finished.getId());
    if (children.isEmpty()) {
      return false;
    }

    boolean scheduled = false;
    for (JobEntity c : children) {
      if (c.getStatus() == JobStatus.PENDING && CHAIN_LOCK_TIME.equals(c.getScheduledTime())) {
        c.setScheduledTime(Instant.now());
        jobCrudStore.save(c);
        log.infof("Chain step %s unlocked (prev=%s)", c.getId(), finished.getId());
        scheduled = true;
      } else if (c.getStatus() == JobStatus.WAITING
          && CHAIN_LOCK_TIME.equals(c.getScheduledTime())) {
        // Signal-waiting chain step: unlock scheduledTime so it runs once the signal arrives,
        // but leave it WAITING — the signal delivery path sets it to PENDING independently.
        c.setScheduledTime(Instant.now());
        jobCrudStore.save(c);
        log.infof(
            "Signal-waiting chain step %s unlocked (signal still pending, prev=%s)",
            c.getId(), finished.getId());
        scheduled = true;
      }
    }
    return scheduled;
  }
}
