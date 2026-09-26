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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.ChainCompletedEvent;
import run.ratchet.api.event.ChainFailedEvent;
import run.ratchet.api.event.ChainStartedEvent;
import run.ratchet.api.event.WorkflowBranchTriggeredEvent;
import run.ratchet.api.exception.KeyProviderUnavailableException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.dto.JobCompletionPlan.DependencyTransition;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Extends {@link ChainScheduler} with conditional branching. Falls back to linear chaining when no
 * workflow conditions are defined.
 *
 * <p>Internal RI service. Public methods inherit the class-level Jakarta Transactions {@code
 * REQUIRED} behavior so workflow branch mutations commit or roll back with the caller's scheduler
 * operation.
 */
@ApplicationScoped
@Transactional
public class WorkflowScheduler extends ChainScheduler {

  private static final Logger log = Logger.getLogger(WorkflowScheduler.class);

  private final WorkflowConditionStore conditionStore;
  private final WorkflowConditionEvaluator conditionEvaluator;
  private final JobTerminalStore jobTerminalStore;
  private final Clock clock;

  protected WorkflowScheduler() {
    super();
    this.conditionStore = null;
    this.conditionEvaluator = null;
    this.jobTerminalStore = null;
    this.clock = null;
  }

  @Inject
  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobTerminalStore jobTerminalStore,
      Instance<WorkflowConditionStore> conditionStore,
      WorkflowConditionEvaluator conditionEvaluator,
      Clock clock,
      InternalEventPublisher eventPublisher,
      AfterCommitRegistrar afterCommitRegistrar) {
    this(
        jobCrudStore,
        jobTerminalStore,
        conditionStore.isResolvable() ? conditionStore.get() : null,
        conditionEvaluator,
        clock,
        eventPublisher,
        afterCommitRegistrar);
  }

  public WorkflowScheduler(
      JobCrudStore jobCrudStore,
      JobTerminalStore jobTerminalStore,
      WorkflowConditionStore conditionStore,
      WorkflowConditionEvaluator conditionEvaluator,
      Clock clock,
      InternalEventPublisher eventPublisher,
      AfterCommitRegistrar afterCommitRegistrar) {
    super(jobCrudStore, jobTerminalStore, clock, eventPublisher, afterCommitRegistrar);
    this.jobTerminalStore = jobTerminalStore;
    this.conditionStore = conditionStore;
    this.conditionEvaluator = conditionEvaluator;
    this.clock = clock;
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  @Override
  public void cancelChain(JobEntity parentJob) {
    super.cancelChain(parentJob);

    List<WorkflowConditionEntity> conditions =
        conditionStore == null
            ? List.of()
            : conditionStore.findConditionsByParentJobId(parentJob.getId());
    Map<UUID, JobEntity> childJobs = loadChildJobs(conditions);

    AtomicInteger canceledCount = new AtomicInteger(0);
    for (WorkflowConditionEntity condition : conditions) {
      JobEntity childJob = childJobs.get(condition.getChildJobId());
      if (childJob == null
          || (childJob.getStatus() != JobStatus.PENDING
              && childJob.getStatus() != JobStatus.WAITING)) {
        continue;
      }
      // Terminal CANCELED transition: cancelJob runs DELETE hot + UPDATE cold +
      // DELETE bkres atomically. setStatus()+save() is rejected by the hot guard.
      if (jobTerminalStore.cancelJob(childJob.getId())) {
        canceledCount.incrementAndGet();
        log.infof(
            "Canceled workflow branch job %s due to parent job %s failure",
            childJob.getId(), parentJob.getId());
      }
    }

    if (canceledCount.get() > 0) {
      log.infof(
          "Canceled %s workflow branch jobs for failed parent job %s",
          canceledCount, parentJob.getId());
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  @Override
  public boolean scheduleNext(JobEntity parentJob) {
    List<WorkflowConditionEntity> conditions =
        conditionStore == null
            ? List.of()
            : conditionStore.findConditionsByParentJobId(parentJob.getId());

    if (conditions.isEmpty()) {
      // Fall back to original linear chaining behavior
      if (parentJob.getStatus() == JobStatus.FAILED) {
        super.cancelChain(parentJob);
        return false;
      } else {
        return super.scheduleNext(parentJob);
      }
    }

    log.infof("Evaluating %s workflow conditions for job %s", conditions.size(), parentJob.getId());

    Map<UUID, JobEntity> childJobs = loadChildJobs(conditions);
    Set<UUID> conditionalChildIds =
        conditions.stream().map(WorkflowConditionEntity::getChildJobId).collect(Collectors.toSet());
    WorkflowConditionEntity scheduledCondition = null;
    for (WorkflowConditionEntity condition : conditions) {
      boolean matched;
      try {
        matched = conditionEvaluator.evaluate(condition, parentJob);
      } catch (KeyProviderUnavailableException | UnsupportedEnvelopeVersionException deferrable) {
        // The branch cannot be decided now, but the data is valid and becomes readable later: a
        // transient key-provider outage (recovers), or a predicate written by a newer Ratchet this
        // node cannot read yet (becomes readable after upgrade). Abort before the post-loop branch
        // cancellation so every PENDING/WAITING branch is preserved, and let the throw unwind the
        // post-execution REQUIRES_NEW. The parent has already completed, so there is no automatic
        // re-evaluation: an operator re-triggers branch scheduling once the key provider is
        // reachable again, or once this node is upgraded.
        log.errorf(
            deferrable,
            "Workflow condition %s for parent %s cannot be decided yet (%s); branch scheduling"
                + " deferred and all branches preserved. Re-trigger scheduling after key"
                + " availability or node upgrade.",
            condition.getId(),
            parentJob.getId(),
            deferrable.getClass().getSimpleName());
        throw deferrable;
      } catch (RuntimeException permanentFailure) {
        // evaluate() lets only a permanent WorkflowConditionConfigurationException escape here
        // (every
        // other evaluation error is already turned into a non-match inside it). The predicate can
        // never be evaluated, so this branch is left unscheduled; cancelUnscheduledBranches below
        // then cancels it durably -- a visible, committed outcome instead of a silent stall.
        // Sibling
        // branches and the linear chain still proceed.
        log.errorf(
            permanentFailure,
            "Workflow condition %s for parent %s is permanently unevaluable; canceling its branch.",
            condition.getId(),
            parentJob.getId());
        continue;
      }
      if (matched && scheduleChildJob(condition, parentJob, childJobs)) {
        scheduledCondition = condition;
        log.infof(
            "Scheduled workflow branch job %s after condition evaluation (type: %s, priority: %s)",
            condition.getChildJobId(),
            condition.getConditionType(),
            condition.getConditionPriority());
        break;
      }
    }

    cancelUnscheduledBranches(conditions, scheduledCondition, childJobs);
    boolean linearChainAdvanced = handleLinearChain(parentJob, conditionalChildIds);

    if (scheduledCondition != null) {
      publishWorkflowBranchTriggered(parentJob, scheduledCondition);
      log.infof(
          "Scheduled workflow branch job %s for parent job %s",
          scheduledCondition.getChildJobId(), parentJob.getId());
      return true;
    }

    log.infof("No workflow conditions met for job %s", parentJob.getId());
    return linearChainAdvanced;
  }

  private boolean handleLinearChain(JobEntity parentJob, Set<UUID> conditionalChildIds) {
    if (parentJob.getStatus() == JobStatus.FAILED) {
      super.cancelChain(parentJob, conditionalChildIds);
      return false;
    }
    return super.scheduleNext(parentJob, conditionalChildIds);
  }

  private void cancelUnscheduledBranches(
      List<WorkflowConditionEntity> conditions,
      WorkflowConditionEntity scheduledCondition,
      Map<UUID, JobEntity> childJobs) {
    UUID scheduledChildId = scheduledCondition == null ? null : scheduledCondition.getChildJobId();
    for (WorkflowConditionEntity condition : conditions) {
      if (Objects.equals(condition.getChildJobId(), scheduledChildId)) {
        continue;
      }
      JobEntity childJob = childJobs.get(condition.getChildJobId());
      if (childJob == null
          || (childJob.getStatus() != JobStatus.PENDING
              && childJob.getStatus() != JobStatus.WAITING)) {
        continue;
      }
      if (jobTerminalStore.cancelJob(childJob.getId())) {
        log.infof(
            "Canceled unmatched workflow branch job %s for condition %s",
            childJob.getId(), condition.getId());
      }
    }
  }

  @SuppressWarnings("java:S1172") // parentJob reserved for future parent context logging
  private boolean scheduleChildJob(
      WorkflowConditionEntity condition, JobEntity parentJob, Map<UUID, JobEntity> childJobs) {
    JobEntity childJob = childJobs.get(condition.getChildJobId());
    if (childJob == null) {
      log.warnf(
          "Child job %s not found for workflow condition %s",
          condition.getChildJobId(), condition.getId());
      return false;
    }
    return scheduleIfPending(condition, childJob);
  }

  @SuppressWarnings("java:S1172") // condition reserved for future context logging
  private boolean scheduleIfPending(WorkflowConditionEntity condition, JobEntity childJob) {
    if (childJob.getStatus() != JobStatus.PENDING && childJob.getStatus() != JobStatus.WAITING) {
      log.warnf(
          "Child job %s is not PENDING or WAITING (current: %s), cannot schedule",
          childJob.getId(), childJob.getStatus());
      return false;
    }

    childJob.setScheduledTime(effective().instant());
    childJob.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    jobCrudStore.save(childJob);
    return true;
  }

  private void publishWorkflowBranchTriggered(
      JobEntity parentJob, WorkflowConditionEntity condition) {
    if (eventPublisher == null) {
      return;
    }
    publishAfterCommit(
        new WorkflowBranchTriggeredEvent(
            parentJob.getId(),
            parentJob.getBusinessKey(),
            parentJob.getRecurringMasterId(),
            parentJob.getPublicJobType(),
            parentJob.getPriority(),
            parentJob.getPickedBy(),
            describeCondition(condition),
            condition.getChildJobId()));
  }

  private String describeCondition(WorkflowConditionEntity condition) {
    if (condition.getConditionExpression() != null
        && !condition.getConditionExpression().isBlank()) {
      return condition.getConditionExpression();
    }
    return condition.getConditionType().name();
  }

  private Map<UUID, JobEntity> loadChildJobs(List<WorkflowConditionEntity> conditions) {
    List<UUID> childIds =
        conditions.stream().map(WorkflowConditionEntity::getChildJobId).distinct().toList();
    if (childIds.isEmpty()) {
      return Map.of();
    }
    return jobCrudStore.findByIds(childIds).stream()
        .collect(Collectors.toMap(JobEntity::getId, Function.identity(), (left, right) -> left));
  }

  /** Computes durable effects without changing any job or publishing pre-commit events. */
  public WorkflowCompletionPlan planCompletion(JobEntity parent, boolean cancelAll) {
    Map<UUID, DependencyTransition> mutations = new LinkedHashMap<>();
    List<Object> events = new ArrayList<>();
    if (cancelAll) {
      planCanceledChain(parent, Set.of(), mutations, events);
      return new WorkflowCompletionPlan(new ArrayList<>(mutations.values()), events, false);
    }
    List<WorkflowConditionEntity> conditions =
        conditionStore == null
            ? List.of()
            : conditionStore.findConditionsByParentJobId(parent.getId());
    Map<UUID, JobEntity> children = loadChildJobs(conditions);
    Set<UUID> conditionalIds =
        conditions.stream().map(WorkflowConditionEntity::getChildJobId).collect(Collectors.toSet());
    WorkflowConditionEntity selected = null;
    for (WorkflowConditionEntity condition : conditions) {
      boolean matched;
      try {
        matched = conditionEvaluator.evaluate(condition, parent);
      } catch (KeyProviderUnavailableException | UnsupportedEnvelopeVersionException deferrable) {
        throw deferrable;
      } catch (RuntimeException permanentFailure) {
        log.warnf(permanentFailure, "Condition %s cannot be evaluated", condition.getId());
        continue;
      }
      JobEntity child = children.get(condition.getChildJobId());
      if (matched && isUnstarted(child)) {
        selected = condition;
        mutations.put(
            child.getId(),
            transition(
                child, child.getStatus(), effective().instant(), JobExecutionType.WORKFLOW_BRANCH));
        events.add(
            new WorkflowBranchTriggeredEvent(
                parent.getId(),
                parent.getBusinessKey(),
                parent.getRecurringMasterId(),
                parent.getPublicJobType(),
                parent.getPriority(),
                parent.getPickedBy(),
                describeCondition(condition),
                child.getId()));
        break;
      }
    }
    UUID selectedId = selected == null ? null : selected.getChildJobId();
    for (WorkflowConditionEntity condition : conditions) {
      JobEntity child = children.get(condition.getChildJobId());
      if (!Objects.equals(selectedId, condition.getChildJobId()) && isUnstarted(child)) {
        mutations.put(
            child.getId(),
            transition(child, JobStatus.CANCELED, child.getScheduledTime(), child.getJobType()));
      }
    }
    boolean newWork = selected != null;
    if (parent.getStatus() == JobStatus.FAILED || parent.getStatus() == JobStatus.CANCELED) {
      planCanceledChain(parent, conditionalIds, mutations, events);
    } else {
      List<JobEntity> linearChildren =
          findAllDependants(parent.getId()).stream()
              .filter(child -> !conditionalIds.contains(child.getId()))
              .toList();
      if (linearChildren.isEmpty() && parent.getJobType() == JobExecutionType.CHAIN_STEP) {
        events.add(
            new ChainCompletedEvent(
                parent.getId(),
                parent.getBusinessKey(),
                parent.getRecurringMasterId(),
                parent.getPublicJobType(),
                parent.getPriority(),
                parent.getPickedBy(),
                findRootJobId(parent)));
      }
      for (JobEntity child : linearChildren) {
        if (isUnstarted(child) && CHAIN_LOCK_TIME.equals(child.getScheduledTime())) {
          mutations.put(
              child.getId(),
              transition(child, child.getStatus(), effective().instant(), child.getJobType()));
          newWork = true;
          if (child.getJobType() == JobExecutionType.CHAIN_STEP
              && parent.getJobType() != JobExecutionType.CHAIN_STEP) {
            events.add(
                new ChainStartedEvent(
                    child.getId(),
                    child.getBusinessKey(),
                    child.getRecurringMasterId(),
                    child.getPublicJobType(),
                    child.getPriority(),
                    child.getPickedBy(),
                    findRootJobId(parent)));
          }
        }
      }
    }
    return new WorkflowCompletionPlan(new ArrayList<>(mutations.values()), events, newWork);
  }

  private void planCanceledChain(
      JobEntity parent,
      Set<UUID> skipped,
      Map<UUID, DependencyTransition> mutations,
      List<Object> events) {
    Deque<UUID> pending = new ArrayDeque<>();
    Set<UUID> visited = new HashSet<>();
    pending.push(parent.getId());
    boolean canceled = false;
    while (!pending.isEmpty()) {
      UUID id = pending.pop();
      if (!visited.add(id)) {
        continue;
      }
      for (JobEntity child : findAllDependants(id)) {
        if (id.equals(parent.getId()) && skipped.contains(child.getId())) {
          continue;
        }
        if (isUnstarted(child)) {
          mutations.put(
              child.getId(),
              transition(child, JobStatus.CANCELED, child.getScheduledTime(), child.getJobType()));
          canceled = true;
        }
        pending.push(child.getId());
      }
    }
    if (canceled || parent.getJobType() == JobExecutionType.CHAIN_STEP) {
      events.add(
          new ChainFailedEvent(
              parent.getId(),
              parent.getBusinessKey(),
              parent.getRecurringMasterId(),
              parent.getPublicJobType(),
              parent.getPriority(),
              parent.getPickedBy(),
              findRootJobId(parent),
              parent.getLastError() != null ? parent.getLastError() : parent.getStatus().name()));
    }
  }

  private static boolean isUnstarted(JobEntity child) {
    return child != null
        && (child.getStatus() == JobStatus.PENDING || child.getStatus() == JobStatus.WAITING);
  }

  private static DependencyTransition transition(
      JobEntity child, JobStatus status, Instant scheduledTime, JobExecutionType type) {
    return new DependencyTransition(
        child.getId(),
        child.getStatus(),
        child.getVersion(),
        child.getScheduledTime(),
        status,
        scheduledTime,
        type);
  }

  public void publishTerminalEvent(Object event) {
    publishAfterCommit(event);
  }

  /** Publish the plan's notifications only after its store composite has committed. */
  public void publishCompletion(WorkflowCompletionPlan plan) {
    plan.events().forEach(this::publishAfterCommit);
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
