package run.ratchet.ri.core;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.BatchMetricsStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;

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

  /** Clears reflection caches on shutdown to prevent classloader leaks. */
  @PreDestroy
  public void clearCaches() {
    HOOK_METHOD_CACHE.clear();
    CLASS_CACHE.clear();
  }

  private final BatchStore batchStore;
  private final JobCrudStore jobCrudStore;
  private final BatchMetricsStore metricsStore;
  private final MetricsCollector metricsCollector;
  private final InternalEventPublisher eventPublisher;
  private final WorkflowScheduler workflowScheduler;

  // Required by CDI proxy
  protected BatchService() {
    this.batchStore = null;
    this.jobCrudStore = null;
    this.metricsStore = null;
    this.metricsCollector = null;
    this.eventPublisher = null;
    this.workflowScheduler = null;
  }

  @Inject
  public BatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      BatchMetricsStore metricsStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler) {
    this.batchStore = batchStore;
    this.jobCrudStore = jobCrudStore;
    this.metricsStore = metricsStore;
    this.metricsCollector = metricsCollector;
    this.eventPublisher = eventPublisher;
    this.workflowScheduler = workflowScheduler;
  }

  /** Increments the failed counter; triggers completion if this was the last child. */
  public void markChildFailed(JobEntity child) {
    update(child, false);
  }

  /** Increments the completed counter; triggers completion if this was the last child. */
  public void markChildSucceeded(JobEntity child) {
    update(child, true);
  }

  /**
   * Recovers batches left in an inconsistent state. Called periodically by {@link
   * BatchRecoveryTimer}.
   *
   * @return the number of batches recovered
   */
  public int recoverStuckBatches() {
    List<Long> recoverableIds = batchStore.findRecoverableBatchIds(100);
    if (recoverableIds.isEmpty()) {
      return 0;
    }

    Map<Long, BatchEntity> batchMap =
        batchStore.findBatchesByIds(recoverableIds).stream()
            .collect(Collectors.toMap(BatchEntity::getId, Function.identity()));

    int recovered = 0;
    for (Long batchId : recoverableIds) {
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
    Class<?> cls =
        CLASS_CACHE.computeIfAbsent(
            payload.target(),
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

    Object instance = cls.getDeclaredConstructor().newInstance();
    method.invoke(instance, ctx);
  }

  private void processBatchCompletion(Long parentId, BatchEntity batch) {
    jobCrudStore
        .findById(parentId)
        .ifPresent(
            parent -> {
              if (parent.getStatus() == JobStatus.PENDING) {
                parent.setStatus(
                    batch.getFailedItems() == 0 ? JobStatus.SUCCEEDED : JobStatus.FAILED);
                jobCrudStore.save(parent);

                metricsStore.finalizeBatchMetrics(parentId);

                metricsStore
                    .findBatchMetrics(parentId)
                    .filter(metrics -> metrics.getTotalDurationMs() != null)
                    .ifPresent(
                        metrics ->
                            metricsCollector.jobCompleted(
                                parentId, parent.getPublicJobType(), metrics.getTotalDurationMs()));

                publishBatchEvent(batch);

                log.info(
                    String.format(
                        "Batch %d completed: %d total, %d succeeded, %d failed",
                        parentId,
                        batch.getTotalItems(),
                        batch.getCompletedItems(),
                        batch.getFailedItems()));

                workflowScheduler.scheduleNext(parent);
              }
            });
  }

  private void publishBatchEvent(BatchEntity batch) {
    eventPublisher.publish(
        new BatchCompletingEvent(
            batch.getId(),
            null, // businessKey
            JobType.BATCH,
            JobPriority.NORMAL,
            "system",
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

  private void update(JobEntity child, boolean jobSuccessful) {
    Long parentId = child.getDependsOn();
    if (parentId == null) {
      return;
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
      return;
    }

    triggerWithProgress(progress.progressHook(), progress);

    if (batchStore.markBatchCompleteIfReady(parentId)) {
      batchStore
          .findBatchById(parentId)
          .ifPresent(batch -> processBatchCompletion(parentId, batch));
    }
  }
}
