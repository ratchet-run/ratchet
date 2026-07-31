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
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobPausedEvent;
import run.ratchet.api.event.JobResumedEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Outcome;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;

/**
 * Cascades pause/resume through job dependency trees via BFS.
 *
 * <p>Internal RI service. Public methods inherit the class-level Jakarta Transactions {@code
 * REQUIRED} behavior so cascade mutations commit or roll back with the caller's scheduler
 * operation.
 */
@ApplicationScoped
@Transactional
public class JobCascadeService {

  private static final int DEPENDANT_PAGE_SIZE = JobCrudStore.DEFAULT_PAGE_LIMIT;

  private final JobCrudStore jobCrudStore;
  private final JobPauseStore jobPauseStore;
  private final InternalEventPublisher eventPublisher;
  private final Clock clock;
  private final AfterCommitRegistrar afterCommitRegistrar;

  protected JobCascadeService() {
    this.jobCrudStore = null;
    this.jobPauseStore = null;
    this.eventPublisher = null;
    this.clock = null;
    this.afterCommitRegistrar = null;
  }

  public JobCascadeService(
      JobCrudStore jobCrudStore,
      JobPauseStore jobPauseStore,
      AfterCommitRegistrar afterCommitRegistrar) {
    this(jobCrudStore, jobPauseStore, null, null, afterCommitRegistrar);
  }

  @Inject
  public JobCascadeService(
      JobCrudStore jobCrudStore,
      JobPauseStore jobPauseStore,
      InternalEventPublisher eventPublisher,
      Clock clock,
      AfterCommitRegistrar afterCommitRegistrar) {
    this.jobCrudStore = jobCrudStore;
    this.jobPauseStore = jobPauseStore;
    this.eventPublisher = eventPublisher;
    this.clock = clock;
    this.afterCommitRegistrar = afterCommitRegistrar;
  }

  /**
   * Iteratively pauses all PENDING children of the given root job using BFS.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   *
   * @return an array of two ints: [pausedCount, skippedCount]
   */
  public int[] pauseChildrenIterative(UUID rootId) {
    int pausedCount = 0;
    int skippedCount = 0;

    Queue<UUID> toProcess = new LinkedList<>();
    Set<UUID> visited = new HashSet<>();

    toProcess.add(rootId);
    visited.add(rootId);

    while (!toProcess.isEmpty()) {
      List<UUID> currentLevel = new ArrayList<>();
      while (!toProcess.isEmpty()) {
        currentLevel.add(toProcess.poll());
      }

      // Find all direct children of the entire level
      for (UUID parentId : currentLevel) {
        List<JobEntity> children = findAllDependants(parentId);
        for (JobEntity child : children) {
          if (!visited.add(child.getId())) {
            continue;
          }

          // Post hot/cold-split: only PENDING (live) can be paused via the hot transition.
          // FAILED is terminal post-split — it has no hot row, so the previous FAILED→PAUSED
          // path is no longer expressible without a resurrection step. Skipping FAILED keeps
          // the cascade behavior conservative; the cleanup is a separate task.
          if (child.getStatus() == JobStatus.PENDING
              && jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)) {
            pausedCount++;
            publishPausedEvent(child);
          } else {
            skippedCount++;
          }

          if (!child.getStatus().isTerminal()) {
            toProcess.add(child.getId());
          }
        }
      }
    }

    return new int[] {pausedCount, skippedCount};
  }

  /**
   * Iteratively resumes all PAUSED children of the given root job using BFS.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   *
   * @return an array of two ints: [resumedCount, skippedCount]
   */
  public int[] resumeChildrenIterative(UUID rootId) {
    int resumedCount = 0;
    int skippedCount = 0;

    Queue<UUID> toProcess = new LinkedList<>();
    Set<UUID> visited = new HashSet<>();

    toProcess.add(rootId);
    visited.add(rootId);

    while (!toProcess.isEmpty()) {
      List<UUID> currentLevel = new ArrayList<>();
      while (!toProcess.isEmpty()) {
        currentLevel.add(toProcess.poll());
      }

      for (UUID parentId : currentLevel) {
        List<JobEntity> children = findAllDependants(parentId);
        for (JobEntity child : children) {
          if (!visited.add(child.getId())) {
            continue;
          }

          if (child.getStatus() == JobStatus.PAUSED
              && jobPauseStore.transitionFromPaused(child.getId(), JobStatus.PENDING)) {
            resumedCount++;
            publishResumedEvent(child);
          } else {
            skippedCount++;
          }

          if (!child.getStatus().isTerminal()) {
            toProcess.add(child.getId());
          }
        }
      }
    }

    return new int[] {resumedCount, skippedCount};
  }

  private List<JobEntity> findAllDependants(UUID parentId) {
    List<JobEntity> dependants = new ArrayList<>();
    int offset = 0;
    while (true) {
      List<JobEntity> page = jobCrudStore.findDependants(parentId, DEPENDANT_PAGE_SIZE, offset);
      dependants.addAll(page);
      if (page.size() < DEPENDANT_PAGE_SIZE) {
        return dependants;
      }
      offset += page.size();
    }
  }

  private void publishPausedEvent(JobEntity job) {
    if (eventPublisher == null) {
      return;
    }
    JobPausedEvent event =
        new JobPausedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getRecurringMasterId(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            now());
    publishAfterCommit(event);
  }

  private void publishResumedEvent(JobEntity job) {
    if (eventPublisher == null) {
      return;
    }
    JobResumedEvent event =
        new JobResumedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getRecurringMasterId(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            now());
    publishAfterCommit(event);
  }

  private Instant now() {
    return clock != null ? clock.instant() : Instant.now();
  }

  private void publishAfterCommit(Object event) {
    if (afterCommitRegistrar.registerAfterCommit(
            () -> eventPublisher.publish(event),
            "After-commit cascade event registration failed; event suppressed: %s")
        == Outcome.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }
}
