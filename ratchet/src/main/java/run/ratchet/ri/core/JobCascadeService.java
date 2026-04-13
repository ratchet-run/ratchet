package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** Cascades pause/resume through job dependency trees via BFS. */
@ApplicationScoped
@Transactional
public class JobCascadeService {

  private final JobCrudStore jobCrudStore;

  protected JobCascadeService() {
    this.jobCrudStore = null;
  }

  @Inject
  public JobCascadeService(JobCrudStore jobCrudStore) {
    this.jobCrudStore = jobCrudStore;
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

          if (child.getStatus() == JobStatus.PENDING || child.getStatus() == JobStatus.FAILED) {
            child.setStatus(JobStatus.PAUSED);
            jobCrudStore.save(child);
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

          if (child.getStatus() == JobStatus.PAUSED) {
            child.setStatus(JobStatus.PENDING);
            if (executeImmediately) {
              child.setScheduledTime(Instant.now());
            }
            jobCrudStore.save(child);
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
