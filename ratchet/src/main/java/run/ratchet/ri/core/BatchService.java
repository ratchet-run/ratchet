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

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.BatchCompletedEvent;
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Tracks batch progress, executes progress hooks, and handles batch completion. Uses atomic
 * operations for concurrent child-job updates and ensures exactly-once completion processing.
 */
@ApplicationScoped
@Transactional
public class BatchService {

  private static final Logger log = Logger.getLogger(BatchService.class);

  private final ConcurrentHashMap<String, Method> hookMethodCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Class<?>> classCache = new ConcurrentHashMap<>();
  private final BatchStore batchStore;
  private final JobCrudStore jobCrudStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;
  private final MetricsCollector metricsCollector;
  private final InternalEventPublisher eventPublisher;
  private final WorkflowScheduler workflowScheduler;
  private final ClassPolicy classPolicy;
  private final BeanResolver beanResolver;
  private final Clock clock;
  private final Instance<BatchService> self;

  private volatile TransactionSynchronizationRegistry txRegistry;

  protected BatchService() {
    this.batchStore = null;
    this.jobCrudStore = null;
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
    this.metricsCollector = null;
    this.eventPublisher = null;
    this.workflowScheduler = null;
    this.classPolicy = null;
    this.beanResolver = null;
    this.clock = null;
    this.self = null;
  }

  public BatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver) {
    this(
        batchStore,
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        metricsCollector,
        eventPublisher,
        workflowScheduler,
        classPolicy,
        beanResolver,
        Clock.systemUTC(),
        null);
  }

  @Inject
  public BatchService(
      Instance<BatchStore> batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver,
      Clock clock,
      Instance<BatchService> self) {
    this(
        batchStore.isResolvable() ? batchStore.get() : null,
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        metricsCollector,
        eventPublisher,
        workflowScheduler,
        classPolicy,
        beanResolver,
        clock,
        self);
  }

  BatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver,
      Clock clock,
      Instance<BatchService> self) {
    this.batchStore = batchStore;
    this.jobCrudStore = jobCrudStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.metricsCollector = metricsCollector;
    this.eventPublisher = eventPublisher;
    this.workflowScheduler = workflowScheduler;
    this.classPolicy = classPolicy;
    this.beanResolver = beanResolver;
    this.clock = clock;
    this.self = self;
  }

  public BatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver,
      Clock clock) {
    this(
        batchStore,
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        metricsCollector,
        eventPublisher,
        workflowScheduler,
        classPolicy,
        beanResolver,
        clock,
        null);
  }

  @PreDestroy
  public void clearCaches() {
    hookMethodCache.clear();
    classCache.clear();
  }

  public boolean markChildFailed(JobEntity child) {
    return update(child, false);
  }

  public boolean markChildSucceeded(JobEntity child) {
    return update(child, true);
  }

  /**
   * Recovers batches left in an inconsistent state. Called periodically by {@link
   * BatchRecoveryTimer}.
   *
   * @return the number of batches recovered
   */
  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  public int recoverStuckBatches() {
    if (batchStore == null) {
      // No BatchStore capability: batch fan-out is unavailable, so there are no batches to recover.
      return 0;
    }
    List<UUID> recoverableIds = batchStore.findRecoverableBatchIds(100);
    if (recoverableIds.isEmpty()) {
      return 0;
    }

    Map<UUID, BatchEntity> batchMap =
        batchStore.findBatchesByIds(recoverableIds).stream()
            .collect(Collectors.toMap(BatchEntity::getId, Function.identity()));

    int recovered = 0;
    Map<UUID, JobEntity> parentMap =
        jobCrudStore.findByIds(recoverableIds).stream()
            .collect(Collectors.toMap(JobEntity::getId, Function.identity()));
    for (UUID batchId : recoverableIds) {
      BatchEntity batch = batchMap.get(batchId);
      JobEntity parent = parentMap.get(batchId);
      if (batch != null && parent != null) {
        try {
          if (recoveryDelegate().recoverCompletedBatch(batchId, batch, parent)) {
            recovered++;
          }
        } catch (RuntimeException ex) {
          log.warnf(ex, "Failed to recover completed batch %s", batchId);
        }
      }
    }
    return recovered;
  }

  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public boolean recoverCompletedBatch(UUID batchId, BatchEntity batch, JobEntity parent) {
    if (!batchStore.markBatchCompleteIfReady(batchId)) {
      return false;
    }
    JobStatus before = parent.getStatus();
    boolean scheduledNext = processBatchCompletion(batchId, batch, parent);
    return scheduledNext || before == JobStatus.PENDING && parent.getStatus() != JobStatus.PENDING;
  }

  private Method resolveHookMethod(Class<?> clazz, JobPayload payload)
      throws NoSuchMethodException {
    String cacheKey = clazz.getName() + "#" + payload.method() + ":" + payload.methodDescriptor();
    Method cached = hookMethodCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        hookMethodCache.put(cacheKey, m);
        return m;
      }
    }
    throw new NoSuchMethodException(
        "Progress hook method "
            + payload.method()
            + " with descriptor "
            + payload.methodDescriptor()
            + " not found in "
            + clazz.getName());
  }

  @SuppressWarnings("java:S112") // Generic exception from reflective method invocation
  private void executeProgressHook(JobPayload payload, BatchContext ctx) throws Exception {
    String targetName = payload.target();
    if (!classPolicy.isAllowed(targetName)) {
      throw new SecurityException(
          "Progress hook target class not allowed by ClassPolicy: " + targetName);
    }

    Class<?> cls = loadProgressHookClass(targetName);
    Method method = resolveHookMethod(cls, payload);

    if (payload.isStatic()) {
      method.invoke(null, ctx);
      return;
    }

    Object instance = beanResolver.resolve(cls);
    method.invoke(instance, ctx);
  }

  private Class<?> loadProgressHookClass(String targetName) {
    Class<?> cached = classCache.get(targetName);
    if (cached != null) {
      return cached;
    }
    Class<?> loaded;
    try {
      loaded = Class.forName(targetName, true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Progress hook target class not found: " + targetName, e);
    }
    Class<?> existing = classCache.putIfAbsent(targetName, loaded);
    return existing == null ? loaded : existing;
  }

  private boolean processBatchCompletion(UUID parentId, BatchEntity batch) {
    return jobCrudStore
        .findById(parentId)
        .map(parent -> processBatchCompletion(parentId, batch, parent))
        .orElse(false);
  }

  private boolean processBatchCompletion(UUID parentId, BatchEntity batch, JobEntity parent) {
    if (parent.getStatus() != JobStatus.PENDING) {
      return false;
    }
    // Skip-execute the parent into terminal SUCCEEDED/FAILED. Post hot/cold-split,
    // save() can't mutate the hot row's status; the equivalent is a synthetic pickup
    // followed by mark-terminal so the hot DELETE + cold UPDATE + bkres DELETE all
    // run atomically through the store.
    if (!jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
      return false;
    }

    boolean succeeded = batch.getFailedItems() == 0;
    Instant nowTs = effective().instant();
    JobStatus terminalStatus = succeeded ? JobStatus.SUCCEEDED : JobStatus.FAILED;
    if (!markBatchParentTerminal(parentId, batch, succeeded, nowTs)) {
      resetSyntheticBatchPickup(parentId);
      return false;
    }
    parent.setStatus(terminalStatus);

    batchStore.finalizeBatchMetrics(parentId);

    Long totalDurationMs =
        batchStore
            .findBatchMetrics(parentId)
            .map(BatchMetricsEntity::getTotalDurationMs)
            .orElse(null);
    if (totalDurationMs != null) {
      metricsCollector.jobCompleted(parentId, parent.getPublicJobType(), totalDurationMs);
    }

    publishBatchEvents(batch, parent, succeeded, totalDurationMs);

    log.infof(
        "Batch %s completed: %d total, %d succeeded, %d failed",
        parentId, batch.getTotalItems(), batch.getCompletedItems(), batch.getFailedItems());

    return workflowScheduler.scheduleNext(parent);
  }

  private boolean markBatchParentTerminal(
      UUID parentId, BatchEntity batch, boolean succeeded, Instant nowTs) {
    try {
      if (succeeded) {
        return jobTerminalStore.markJobSucceededMinimal(parentId, nowTs, nowTs, 0L, 0L);
      }
      return jobTerminalStore.markJobFailedTerminal(
          parentId, "Batch completed with " + batch.getFailedItems() + " failed children", 0);
    } catch (RuntimeException e) {
      resetSyntheticBatchPickup(parentId, e);
      throw e;
    }
  }

  private void resetSyntheticBatchPickup(UUID parentId) {
    resetSyntheticBatchPickup(parentId, null);
  }

  private void resetSyntheticBatchPickup(UUID parentId, RuntimeException cause) {
    try {
      if (jobBatchStatusStore.resetRunningJob(
          parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
        return;
      }
    } catch (RuntimeException resetFailure) {
      if (cause != null) {
        resetFailure.addSuppressed(cause);
      }
      throw resetFailure;
    }

    IllegalStateException failure =
        new IllegalStateException(
            "Batch parent "
                + parentId
                + " synthetic pickup could not be reset after terminal transition failure");
    if (cause != null) {
      failure.addSuppressed(cause);
    }
    throw failure;
  }

  private void publishBatchEvents(
      BatchEntity batch, JobEntity parent, boolean succeeded, Long totalDurationMs) {
    Runnable publish = () -> publishBatchEventsNow(batch, parent, succeeded, totalDurationMs);
    if (!registerAfterCommit(publish)) {
      publish.run();
    }
  }

  private void publishBatchEventsNow(
      BatchEntity batch, JobEntity parent, boolean succeeded, Long totalDurationMs) {
    eventPublisher.publish(
        new BatchCompletingEvent(
            batch.getId(),
            parent.getBusinessKey(),
            parent.getPublicJobType(),
            parent.getPriority(),
            parent.getPickedBy(),
            batch.getTotalItems(),
            batch.getCompletedItems(),
            batch.getFailedItems()));
    eventPublisher.publish(
        new BatchCompletedEvent(
            batch.getId(),
            parent.getBusinessKey(),
            parent.getPublicJobType(),
            parent.getPriority(),
            parent.getPickedBy(),
            batch.getTotalItems(),
            batch.getCompletedItems(),
            batch.getFailedItems()));
    if (succeeded) {
      eventPublisher.publish(
          new JobCompletedEvent(
              parent.getId(),
              parent.getBusinessKey(),
              parent.getPublicJobType(),
              parent.getPriority(),
              parent.getPickedBy(),
              totalDurationMs));
      return;
    }
    eventPublisher.publish(
        new JobFailedEvent(
            parent.getId(),
            parent.getBusinessKey(),
            parent.getPublicJobType(),
            parent.getPriority(),
            parent.getPickedBy(),
            "Batch completed with " + batch.getFailedItems() + " failed children",
            parent.getAttempts()));
  }

  private boolean registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit batch completing event registration failed; publishing immediately: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          reg = JobWakeupService.lookupTxRegistry(log);
          txRegistry = reg;
        }
      }
    }
    return reg;
  }

  void setTxRegistryForTesting(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
  }

  private void trigger(BatchEntity batch) {
    JobPayload hookPayload = batch.getProgressHook();
    if (hookPayload == null) {
      return;
    }

    BatchContext ctx =
        new BatchContext(
            batch.getId(),
            batch.getTotalItems(),
            batch.getCompletedItems(),
            batch.getFailedItems());

    try {
      executeProgressHook(hookPayload, ctx);
    } catch (Exception ex) {
      log.warnf(ex, "Progress hook for batch %s threw exception", batch.getId());
    }
  }

  private void triggerWithProgress(JobPayload hookPayload, BatchProgress progress) {
    if (hookPayload == null) {
      return;
    }

    BatchContext ctx =
        new BatchContext(
            progress.batchId(),
            progress.totalItems(),
            progress.completedItems(),
            progress.failedItems());

    try {
      executeProgressHook(hookPayload, ctx);
    } catch (Exception ex) {
      log.warnf(ex, "Progress hook for batch %s threw exception", progress.batchId());
    }
  }

  private boolean update(JobEntity child, boolean jobSuccessful) {
    UUID parentId = child.getDependsOn();
    if (parentId == null) {
      return false;
    }

    if (jobSuccessful && child.getExecutionDurationMs() != null) {
      batchStore.addChildExecutionTime(parentId, child.getExecutionDurationMs());
    }

    BatchProgress progress;
    if (jobSuccessful) {
      progress = batchStore.incrementCompletedAtomic(parentId);
    } else {
      progress = batchStore.incrementFailedAtomic(parentId);
    }

    if (progress == null) {
      log.warnf("Batch %s not found during update", parentId);
      return false;
    }

    triggerWithProgress(progress.progressHook(), progress);

    if (batchStore.markBatchCompleteIfReady(parentId)) {
      // markBatchCompleteIfReady can have more than one apparent winner under concurrent commits.
      // The parent pickup CAS in processBatchCompletion is the exactly-once gate.
      return processBatchCompletion(parentId, completionSnapshot(parentId, progress));
    }
    return false;
  }

  private BatchEntity completionSnapshot(UUID parentId, BatchProgress progress) {
    return batchStore
        .findBatchById(parentId)
        .orElseGet(
            () -> {
              log.warnf(
                  "Batch %s not found after completion marker was set; using progress snapshot",
                  parentId);
              return batchFromProgress(progress);
            });
  }

  private BatchEntity batchFromProgress(BatchProgress progress) {
    BatchEntity batch = new BatchEntity();
    batch.setId(progress.batchId());
    batch.setTotalItems(progress.totalItems());
    batch.setCompletedItems(progress.completedItems());
    batch.setFailedItems(progress.failedItems());
    batch.setProgressHook(progress.progressHook());
    return batch;
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  private BatchService recoveryDelegate() {
    return self != null && !self.isUnsatisfied() ? self.get() : this;
  }
}
