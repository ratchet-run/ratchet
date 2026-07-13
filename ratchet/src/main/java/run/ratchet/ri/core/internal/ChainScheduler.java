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
package run.ratchet.ri.core.internal;

import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.ChainCompletedEvent;
import run.ratchet.api.event.ChainFailedEvent;
import run.ratchet.api.event.ChainStartedEvent;
import run.ratchet.ri.core.internal.JobWakeupService.AfterCommitRegistrationResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Propagates success/failure through chained job dependencies.
 *
 * <p>Internal RI service. Public methods inherit the class-level Jakarta Transactions {@code
 * REQUIRED} behavior so chain state changes commit or roll back with the caller's scheduler
 * operation.
 */
@Transactional
public class ChainScheduler {

  /**
   * Sentinel timestamp that keeps chain jobs invisible to the poller until predecessors complete.
   */
  public static final Instant CHAIN_LOCK_TIME = Instant.parse("9999-12-31T23:59:59Z");

  private static final Logger log = Logger.getLogger(ChainScheduler.class);
  private static final int DEPENDANT_PAGE_SIZE = JobCrudStore.DEFAULT_PAGE_LIMIT;

  protected final JobCrudStore jobCrudStore;
  protected final JobTerminalStore jobTerminalStore;
  protected final InternalEventPublisher eventPublisher;
  private final Clock clock;

  private volatile TransactionSynchronizationRegistry txRegistry;

  protected ChainScheduler() {
    this.jobCrudStore = null;
    this.jobTerminalStore = null;
    this.eventPublisher = null;
    this.clock = null;
  }

  public ChainScheduler(JobCrudStore jobCrudStore, JobTerminalStore jobTerminalStore) {
    this(jobCrudStore, jobTerminalStore, Clock.systemUTC(), null);
  }

  ChainScheduler(JobCrudStore jobCrudStore, JobTerminalStore jobTerminalStore, Clock clock) {
    this(jobCrudStore, jobTerminalStore, clock, null);
  }

  ChainScheduler(
      JobCrudStore jobCrudStore,
      JobTerminalStore jobTerminalStore,
      Clock clock,
      InternalEventPublisher eventPublisher) {
    this.jobCrudStore = jobCrudStore;
    this.jobTerminalStore = jobTerminalStore;
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.eventPublisher = eventPublisher;
  }

  public void cancelChain(JobEntity failed) {
    cancelChain(failed, Set.of());
  }

  protected void cancelChain(JobEntity failed, Set<UUID> skippedDirectChildren) {
    Deque<UUID> stack = new ArrayDeque<>();
    stack.push(failed.getId());
    boolean canceled = false;

    while (!stack.isEmpty()) {
      UUID parentId = stack.pop();
      List<JobEntity> children = findAllDependants(parentId);
      for (JobEntity child : children) {
        if (parentId.equals(failed.getId()) && skippedDirectChildren.contains(child.getId())) {
          continue;
        }
        JobStatus status = child.getStatus();
        if (status == JobStatus.PENDING || status == JobStatus.WAITING) {
          // Terminal CANCELED transition: cancelJob runs DELETE hot + UPDATE cold +
          // DELETE bkres atomically. setStatus()+save() is rejected by the hot guard.
          if (jobTerminalStore.cancelJob(child.getId())) {
            log.warnf("Chain step %s canceled (ancestor failed %s)", child.getId(), failed.getId());
            canceled = true;
          }
        }
        stack.push(child.getId());
      }
    }
    if (canceled || failed.getJobType() == JobExecutionType.CHAIN_STEP) {
      publishChainFailed(failed);
    }
  }

  public boolean scheduleNext(JobEntity finished) {
    return scheduleNext(finished, Set.of());
  }

  protected boolean scheduleNext(JobEntity finished, Set<UUID> skippedDirectChildren) {
    List<JobEntity> children = findAllDependants(finished.getId());
    boolean hasEligibleChild = false;
    for (JobEntity child : children) {
      if (!skippedDirectChildren.contains(child.getId())) {
        hasEligibleChild = true;
        break;
      }
    }
    if (!hasEligibleChild) {
      if (finished.getJobType() == JobExecutionType.CHAIN_STEP) {
        publishChainCompleted(finished);
      }
      return false;
    }

    boolean scheduled = false;
    for (JobEntity c : children) {
      if (skippedDirectChildren.contains(c.getId())) {
        continue;
      }
      if (c.getStatus() == JobStatus.PENDING && CHAIN_LOCK_TIME.equals(c.getScheduledTime())) {
        c.setScheduledTime(effective().instant());
        jobCrudStore.save(c);
        log.infof("Chain step %s unlocked (prev=%s)", c.getId(), finished.getId());
        publishChainStartedIfFirstStep(finished, c);
        scheduled = true;
      } else if (c.getStatus() == JobStatus.WAITING
          && CHAIN_LOCK_TIME.equals(c.getScheduledTime())) {
        // Signal-waiting chain step: unlock scheduledTime so it runs once the signal arrives,
        // but leave it WAITING — the signal delivery path sets it to PENDING independently.
        c.setScheduledTime(effective().instant());
        jobCrudStore.save(c);
        log.infof(
            "Signal-waiting chain step %s unlocked (signal still pending, prev=%s)",
            c.getId(), finished.getId());
        publishChainStartedIfFirstStep(finished, c);
        scheduled = true;
      }
    }
    return scheduled;
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  protected List<JobEntity> findAllDependants(UUID parentId) {
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

  private void publishChainStartedIfFirstStep(JobEntity finished, JobEntity child) {
    if (eventPublisher == null
        || child.getJobType() != JobExecutionType.CHAIN_STEP
        || finished.getJobType() == JobExecutionType.CHAIN_STEP) {
      return;
    }
    publishAfterCommit(
        new ChainStartedEvent(
            child.getId(),
            child.getBusinessKey(),
            child.getPublicJobType(),
            child.getPriority(),
            child.getPickedBy(),
            findRootJobId(finished)));
  }

  private void publishChainCompleted(JobEntity finished) {
    if (eventPublisher == null) {
      return;
    }
    publishAfterCommit(
        new ChainCompletedEvent(
            finished.getId(),
            finished.getBusinessKey(),
            finished.getPublicJobType(),
            finished.getPriority(),
            finished.getPickedBy(),
            findRootJobId(finished)));
  }

  private void publishChainFailed(JobEntity failed) {
    if (eventPublisher == null) {
      return;
    }
    publishAfterCommit(
        new ChainFailedEvent(
            failed.getId(),
            failed.getBusinessKey(),
            failed.getPublicJobType(),
            failed.getPriority(),
            failed.getPickedBy(),
            findRootJobId(failed),
            failed.getLastError()));
  }

  /**
   * Publishes a chain or workflow state-change event only after the surrounding transaction
   * commits. Subclasses use the same seam so branch events cannot escape a rolled-back chain
   * mutation.
   */
  protected void publishAfterCommit(Object event) {
    if (eventPublisher == null) {
      return;
    }
    Runnable publish = () -> eventPublisher.publish(event);
    if (JobWakeupService.registerAfterCommit(
            resolveTxRegistry(),
            publish,
            log,
            "After-commit chain/workflow event registration failed; event suppressed: %s")
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      publish.run();
    }
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry registry = txRegistry;
    if (registry == null) {
      synchronized (this) {
        registry = txRegistry;
        if (registry == null) {
          registry = JobWakeupService.lookupTxRegistry(log);
          txRegistry = registry;
        }
      }
    }
    return registry;
  }

  void setTxRegistryForTesting(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
  }

  private UUID findRootJobId(JobEntity job) {
    UUID rootId = job.getId();
    UUID parentId = job.getDependsOn();
    Set<UUID> seen = new HashSet<>();
    while (parentId != null && seen.add(parentId)) {
      JobEntity parent = jobCrudStore.findById(parentId).orElse(null);
      if (parent == null) {
        return parentId;
      }
      rootId = parent.getId();
      parentId = parent.getDependsOn();
    }
    return rootId;
  }
}
