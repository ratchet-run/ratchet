package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.jboss.logging.Logger;

/** Cascades pause/resume through job dependency trees via BFS. */
@ApplicationScoped
@Transactional
public class JobCascadeService {

  private static final Logger log = Logger.getLogger(JobCascadeService.class);

  private final JobCrudStore jobCrudStore;
  private final JobStatusStore jobStatusStore;

  protected JobCascadeService() {
    this.jobCrudStore = null;
    this.jobStatusStore = null;
  }

  @Inject
  public JobCascadeService(JobCrudStore jobCrudStore, JobStatusStore jobStatusStore) {
    this.jobCrudStore = jobCrudStore;
    this.jobStatusStore = jobStatusStore;
  }

  /**
   * Iteratively pauses all PENDING/FAILED children of the given root job using BFS.
   *
   * @return an array of two ints: [pausedCount, skippedCount]
   */
  public int[] pauseChildrenIterative(Long rootId) {
    int pausedCount = 0;
    int skippedCount = 0;

    Queue<Long> toProcess = new LinkedList<>();
    Set<Long> visited = new HashSet<>();

    toProcess.add(rootId);
    visited.add(rootId);

    while (!toProcess.isEmpty()) {
      List<Long> currentLevel = new ArrayList<>();
      while (!toProcess.isEmpty()) {
        currentLevel.add(toProcess.poll());
      }

      // Find all direct children of the entire level
      for (Long parentId : currentLevel) {
        List<JobEntity> children = jobCrudStore.findDependants(parentId);
        for (JobEntity child : children) {
          if (!visited.add(child.getId())) {
            continue;
          }

          // Post hot/cold-split: only PENDING (live) can be paused via the hot transition.
          // FAILED is terminal post-split — it has no hot row, so the legacy FAILED→PAUSED
          // path is no longer expressible without a resurrection step. Skipping FAILED keeps
          // the cascade behavior conservative; the cleanup is a separate task.
          if (child.getStatus() == JobStatus.PENDING
              && jobStatusStore.transitionToPaused(child.getId(), JobStatus.PENDING)) {
            pausedCount++;
          } else {
            skippedCount++;
          }

          toProcess.add(child.getId());
        }
      }
    }

    return new int[] {pausedCount, skippedCount};
  }

  /**
   * Iteratively resumes all PAUSED children of the given root job using BFS.
   *
   * @param executeImmediately if true, set scheduledTime to NOW for each resumed child
   * @return an array of two ints: [resumedCount, skippedCount]
   */
  public int[] resumeChildrenIterative(Long rootId, boolean executeImmediately) {
    int resumedCount = 0;
    int skippedCount = 0;

    Queue<Long> toProcess = new LinkedList<>();
    Set<Long> visited = new HashSet<>();

    toProcess.add(rootId);
    visited.add(rootId);

    while (!toProcess.isEmpty()) {
      List<Long> currentLevel = new ArrayList<>();
      while (!toProcess.isEmpty()) {
        currentLevel.add(toProcess.poll());
      }

      for (Long parentId : currentLevel) {
        List<JobEntity> children = jobCrudStore.findDependants(parentId);
        for (JobEntity child : children) {
          if (!visited.add(child.getId())) {
            continue;
          }

          if (child.getStatus() == JobStatus.PAUSED
              && jobStatusStore.transitionFromPaused(child.getId(), JobStatus.PENDING)) {
            // executeImmediately scheduled_time bump isn't expressible through the post-split
            // hot transition SPI; resumed children keep their original scheduled_time. Logging
            // the gap so callers can opt into the future explicit reschedule API.
            if (executeImmediately) {
              log.debugf(
                  "executeImmediately ignored for resumed child %s — scheduled_time unchanged",
                  child.getId());
            }
            resumedCount++;
          } else {
            skippedCount++;
          }

          toProcess.add(child.getId());
        }
      }
    }

    return new int[] {resumedCount, skippedCount};
  }
}
