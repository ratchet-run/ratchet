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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.objectweb.asm.Type;

/**
 * Service responsible for batch job management and processing within the job scheduler framework.
 * This class orchestrates the execution of batch operations, which consist of a parent job that
 * manages multiple child jobs executed in parallel or sequentially.
 *
 * <p>Key responsibilities include:
 *
 * <ul>
 *   <li>Creating and managing batch parent jobs that coordinate child job execution
 *   <li>Tracking batch progress through atomic counters for completed and failed items
 *   <li>Executing progress hooks stored in the database for real-time batch monitoring
 *   <li>Handling batch completion logic with proper transaction boundaries
 *   <li>Recovering stuck batches due to node failures or crashes
 *   <li>Publishing batch events and collecting metrics for monitoring
 * </ul>
 *
 * <p>Progress hooks are stored as {@link JobPayload} in the database, allowing any cluster node to
 * execute callbacks when processing child jobs. This ensures hooks survive node crashes and work
 * correctly in distributed environments.
 *
 * <p>The service uses optimistic locking and atomic operations to handle concurrent updates from
 * multiple child jobs completing simultaneously. It ensures exactly-once batch completion
 * processing through database-level constraints.
 *
 * <p>Thread Safety: This service is thread-safe and designed for concurrent access from multiple
 * worker threads processing child jobs.
 *
 * @see BatchStore for batch persistence operations
 * @see JobCrudStore for job entity management
 * @see BatchContext for batch progress information
 */
@ApplicationScoped
@Transactional
public class BatchService {

  private static final Logger log = Logger.getLogger(BatchService.class.getName());

  /**
   * Cache of resolved hook methods keyed by "className#methodName:descriptor" to avoid repeated
   * {@link Class#forName} and {@link Class#getMethods()} calls on every child completion.
   */
  private static final ConcurrentHashMap<String, Method> HOOK_METHOD_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Cache of resolved classes keyed by fully-qualified class name to avoid repeated {@link
   * Class#forName} calls on every progress hook execution.
   */
  private static final ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

  /** Store for batch entity CRUD operations and atomic progress updates. */
  private final BatchStore batchStore;

  /** Store for job entity operations, used to update parent job status on batch completion. */
  private final JobCrudStore jobCrudStore;

  /** Store for batch metrics tracking, including child execution time aggregation. */
  private final BatchMetricsStore metricsStore;

  /** Collector for exposing batch metrics to monitoring systems. */
  private final MetricsCollector metricsCollector;

  /** Publisher for batch lifecycle events. */
  private final InternalEventPublisher eventPublisher;

  /** Scheduler for triggering workflow branches after batch completion. */
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

  /**
   * Marks a child job as failed and updates the parent batch's progress counters.
   *
   * <p>This method is called when a child job in a batch fails permanently (after exhausting
   * retries). It atomically increments the batch's failed item counter and triggers progress hooks
   * if configured. If this was the last child to complete, batch completion processing is
   * initiated.
   *
   * <p>The update operation is atomic to handle concurrent completion of multiple child jobs. Only
   * one thread will successfully process batch completion even if multiple children complete
   * simultaneously.
   *
   * @param child the child job entity that failed; must have a non-null {@code dependsOn} field
   *     pointing to the parent batch job
   */
  public void markChildFailed(JobEntity child) {
    update(child, false);
  }

  /**
   * Marks a child job as succeeded and updates the parent batch's progress counters.
   *
   * <p>This method is called when a child job in a batch completes successfully. It atomically
   * increments the batch's completed item counter and triggers progress hooks if configured. Child
   * execution time is recorded for batch metrics. If this was the last child to complete, batch
   * completion processing is initiated.
   *
   * <p>The update operation is atomic to handle concurrent completion of multiple child jobs. Only
   * one thread will successfully process batch completion even if multiple children complete
   * simultaneously.
   *
   * @param child the child job entity that succeeded; must have a non-null {@code dependsOn} field
   *     pointing to the parent batch job
   */
  public void markChildSucceeded(JobEntity child) {
    update(child, true);
  }

  /**
   * Recovers batches that may have been left in an inconsistent state due to node failures or other
   * issues. Called by {@link BatchRecoveryTimer} periodically to ensure batch completion processing
   * doesn't get stuck.
   *
   * <p>This handles edge cases where:
   *
   * <ul>
   *   <li>A node crashed after all children completed but before marking completion
   *   <li>Network partition caused completion flag update to fail
   *   <li>Transaction rollback left batch in inconsistent state
   * </ul>
   *
   * @return the number of batches that were recovered
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

  /**
   * Resolves the hook method from the class using the payload's method descriptor. The method must
   * accept a single {@link BatchContext} parameter.
   *
   * @param clazz the class containing the method
   * @param payload the payload with method name and descriptor
   * @return the resolved method
   * @throws NoSuchMethodException if no matching method is found
   */
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

  /**
   * Executes a progress hook by resolving the target class and invoking the method with
   * BatchContext.
   *
   * <p>The hook is stored as a {@link JobPayload} containing:
   *
   * <ul>
   *   <li>Target class name
   *   <li>Method name (must accept {@link BatchContext} as parameter)
   *   <li>Method descriptor for signature matching
   * </ul>
   *
   * @param payload the hook payload containing method reference info
   * @param ctx the batch context to pass to the hook method
   * @throws Exception if class lookup or method invocation fails
   */
  @SuppressWarnings("java:S112") // Generic exception from reflective method invocation
  private void executeProgressHook(JobPayload payload, BatchContext ctx) throws Exception {
    Class<?> cls =
        CLASS_CACHE.computeIfAbsent(
            payload.target(),
            name -> {
              try {
                return Class.forName(name);
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

  /**
   * Processes batch completion logic after all child jobs have finished.
   *
   * <p>This method is called only by the thread that successfully marks the batch as complete via
   * the {@code markBatchCompleteIfReady} atomic operation, ensuring exactly-once processing of
   * batch completion even in a distributed environment.
   *
   * <p>Completion processing includes:
   *
   * <ol>
   *   <li>Updating the parent job status to SUCCEEDED (if no failures) or FAILED
   *   <li>Finalizing batch metrics (duration, throughput calculations)
   *   <li>Recording metrics to the metrics collector for monitoring
   *   <li>Publishing a batch completion event for external listeners
   *   <li>Logging a summary of batch execution results
   * </ol>
   *
   * @param parentId the database ID of the parent batch job
   * @param batch the batch entity containing progress counters and configuration
   */
  private void processBatchCompletion(Long parentId, BatchEntity batch) {
    jobCrudStore
        .findById(parentId)
        .ifPresent(
            parent -> {
              if (parent.getStatus() == JobStatus.PENDING) {
                parent.setStatus(
                    batch.getFailedItems() == 0 ? JobStatus.SUCCEEDED : JobStatus.FAILED);
                jobCrudStore.save(parent);

                // Finalize batch metrics
                metricsStore.finalizeBatchMetrics(parentId);

                // Record batch metrics to metrics collector
                metricsStore
                    .findBatchMetrics(parentId)
                    .filter(metrics -> metrics.getTotalDurationMs() != null)
                    .ifPresent(
                        metrics ->
                            metricsCollector.jobCompleted(
                                parentId, parent.getPublicJobType(), metrics.getTotalDurationMs()));

                // Publish batch completion event
                publishBatchEvent(batch);

                log.info(
                    String.format(
                        "Batch %d completed: %d total, %d succeeded, %d failed",
                        parentId,
                        batch.getTotalItems(),
                        batch.getCompletedItems(),
                        batch.getFailedItems()));

                // Trigger workflow branches (e.g. success/failure callbacks)
                workflowScheduler.scheduleNext(parent);
              }
            });
  }

  /**
   * Publishes a batch completion event via the internal event publisher.
   *
   * @param batch the batch entity containing current progress counters
   */
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

  /**
   * Triggers the execution of a progress hook stored in the batch entity.
   *
   * @param batch the batch entity containing the progress hook payload
   */
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
      log.warning(
          "Progress hook for batch " + batch.getId() + " threw exception: " + ex.getMessage());
    }
  }

  /**
   * Triggers a progress hook using atomically-obtained progress values.
   *
   * @param hookPayload the progress hook payload, or null if no hook configured
   * @param progress the atomic snapshot of batch progress from the increment operation
   */
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
      log.warning(
          "Progress hook for batch " + progress.batchId() + " threw exception: " + ex.getMessage());
    }
  }

  /**
   * Updates the status and progress of a parent batch and its associated job based on the
   * completion or failure of a child job. Uses atomic operations to prevent race conditions during
   * concurrent child job completions.
   *
   * @param child The child job entity whose completion/failure triggers the update.
   * @param jobSuccessful Indicates whether the child job completed successfully (true) or failed
   *     (false).
   */
  private void update(JobEntity child, boolean jobSuccessful) {
    Long parentId = child.getDependsOn();
    if (parentId == null) {
      return;
    }

    // Track child execution time for metrics (before atomic update)
    if (jobSuccessful && child.getExecutionDurationMs() != null) {
      metricsStore.addChildExecutionTime(parentId, child.getExecutionDurationMs());
    }

    // Atomically update batch progress and get snapshot of current state
    BatchProgress progress;
    if (jobSuccessful) {
      progress = batchStore.incrementCompletedAtomic(parentId);
    } else {
      progress = batchStore.incrementFailedAtomic(parentId);
    }

    if (progress == null) {
      log.warning("Batch " + parentId + " not found during update");
      return;
    }

    // Trigger progress hooks using the atomic snapshot values (progressHook included in snapshot)
    triggerWithProgress(progress.progressHook(), progress);

    // Atomically check if batch is complete and mark as processed
    if (batchStore.markBatchCompleteIfReady(parentId)) {
      batchStore
          .findBatchById(parentId)
          .ifPresent(batch -> processBatchCompletion(parentId, batch));
    }
  }
}
