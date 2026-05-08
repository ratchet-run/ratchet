package run.ratchet.ri.core;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
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
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchMetricsStore;
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

  private static final ConcurrentHashMap<String, Method> HOOK_METHOD_CACHE =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
  private final BatchStore batchStore;
  private final JobCrudStore jobCrudStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobTerminalStore jobTerminalStore;
  private final BatchMetricsStore metricsStore;
  private final MetricsCollector metricsCollector;
  private final InternalEventPublisher eventPublisher;
  private final WorkflowScheduler workflowScheduler;
  private final ClassPolicy classPolicy;
  private final BeanResolver beanResolver;

  protected BatchService() {
    this.batchStore = null;
    this.jobCrudStore = null;
    this.jobBatchStatusStore = null;
    this.jobTerminalStore = null;
    this.metricsStore = null;
    this.metricsCollector = null;
    this.eventPublisher = null;
    this.workflowScheduler = null;
    this.classPolicy = null;
    this.beanResolver = null;
  }

  @Inject
  public BatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      BatchMetricsStore metricsStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver) {
    this.batchStore = batchStore;
    this.jobCrudStore = jobCrudStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobTerminalStore = jobTerminalStore;
    this.metricsStore = metricsStore;
    this.metricsCollector = metricsCollector;
    this.eventPublisher = eventPublisher;
    this.workflowScheduler = workflowScheduler;
    this.classPolicy = classPolicy;
    this.beanResolver = beanResolver;
  }

  @PreDestroy
  public void clearCaches() {
    HOOK_METHOD_CACHE.clear();
    CLASS_CACHE.clear();
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
  public int recoverStuckBatches() {
    List<UUID> recoverableIds = batchStore.findRecoverableBatchIds(100);
    if (recoverableIds.isEmpty()) {
      return 0;
    }

    Map<UUID, BatchEntity> batchMap =
        batchStore.findBatchesByIds(recoverableIds).stream()
            .collect(Collectors.toMap(BatchEntity::getId, Function.identity()));

    int recovered = 0;
    for (UUID batchId : recoverableIds) {
      BatchEntity batch = batchMap.get(batchId);
      if (batch == null) {
        continue;
      }

      if (batchStore.markBatchCompleteIfReady(batchId)) {
        processBatchCompletion(batchId, batch);
        recovered++;
      }
    }
    return recovered;
  }

  private Method resolveHookMethod(Class<?> clazz, JobPayload payload)
      throws NoSuchMethodException {
    String cacheKey = clazz.getName() + "#" + payload.method() + ":" + payload.methodDescriptor();
    Method cached = HOOK_METHOD_CACHE.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        HOOK_METHOD_CACHE.put(cacheKey, m);
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

    Class<?> cls =
        CLASS_CACHE.computeIfAbsent(
            targetName,
            name -> {
              try {
                return Class.forName(name, true, Thread.currentThread().getContextClassLoader());
              } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Progress hook target class not found: " + name, e);
              }
            });
    Method method = resolveHookMethod(cls, payload);

    if (payload.isStatic()) {
      method.invoke(null, ctx);
      return;
    }

    Object instance = beanResolver.resolve(cls);
    method.invoke(instance, ctx);
  }

  private boolean processBatchCompletion(UUID parentId, BatchEntity batch) {
    return jobCrudStore
        .findById(parentId)
        .map(
            parent -> {
              if (parent.getStatus() == JobStatus.PENDING) {
                // Skip-execute the parent into terminal SUCCEEDED/FAILED. Post hot/cold-split,
                // save() can't mutate the hot row's status; the equivalent is a synthetic pickup
                // followed by mark-terminal so the hot DELETE + cold UPDATE + bkres DELETE all
                // run atomically through the store.
                if (!jobBatchStatusStore.tryPickUpJob(
                    parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID)) {
                  return false;
                }

                boolean succeeded = batch.getFailedItems() == 0;
                Instant nowTs = Instant.now();
                if (succeeded) {
                  jobTerminalStore.markJobSucceededMinimal(parentId, nowTs, nowTs, 0L, 0L);
                  parent.setStatus(JobStatus.SUCCEEDED);
                } else {
                  jobTerminalStore.markJobFailedTerminal(
                      parentId,
                      "Batch completed with " + batch.getFailedItems() + " failed children",
                      0);
                  parent.setStatus(JobStatus.FAILED);
                }

                metricsStore.finalizeBatchMetrics(parentId);

                metricsStore
                    .findBatchMetrics(parentId)
                    .filter(metrics -> metrics.getTotalDurationMs() != null)
                    .ifPresent(
                        metrics ->
                            metricsCollector.jobCompleted(
                                parentId, parent.getPublicJobType(), metrics.getTotalDurationMs()));

                publishBatchEvent(batch, parent);

                log.info(
                    String.format(
                        "Batch %s completed: %d total, %d succeeded, %d failed",
                        parentId,
                        batch.getTotalItems(),
                        batch.getCompletedItems(),
                        batch.getFailedItems()));

                return workflowScheduler.scheduleNext(parent);
              }
              return false;
            })
        .orElse(false);
  }

  private void publishBatchEvent(BatchEntity batch, JobEntity parent) {
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
      log.warnf("Progress hook for batch %s threw exception: %s", batch.getId(), ex.getMessage());
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
      log.warnf(
          "Progress hook for batch %s threw exception: %s", progress.batchId(), ex.getMessage());
    }
  }

  private boolean update(JobEntity child, boolean jobSuccessful) {
    UUID parentId = child.getDependsOn();
    if (parentId == null) {
      return false;
    }

    if (jobSuccessful && child.getExecutionDurationMs() != null) {
      metricsStore.addChildExecutionTime(parentId, child.getExecutionDurationMs());
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
      return batchStore
          .findBatchById(parentId)
          .map(batch -> processBatchCompletion(parentId, batch))
          .orElse(false);
    }
    return false;
  }
}
