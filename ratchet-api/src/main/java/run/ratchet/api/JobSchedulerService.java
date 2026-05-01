package run.ratchet.api;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Primary entry point for scheduling background jobs.
 *
 * <p>Implementations provide job creation, batch processing, recurring scheduling, and job
 * replacement capabilities.
 *
 * <h2>Transaction contract</h2>
 *
 * <p>This interface intentionally does not carry a type-level transaction annotation.
 * Implementations MUST honour the per-method transaction attribute documented on each method below.
 * The attribute values follow Jakarta Transactions semantics ({@code REQUIRED}, {@code
 * REQUIRES_NEW}, {@code SUPPORTS}, {@code NOT_SUPPORTED}):
 *
 * <ul>
 *   <li><b>{@code REQUIRED}</b> — the operation MUST execute within a transaction. Implementations
 *       MUST enter a new transaction if the caller has none and MUST join the caller's transaction
 *       if one is active.
 *   <li><b>{@code SUPPORTS}</b> — the operation MUST NOT start a transaction. Implementations MUST
 *       join the caller's transaction if one is active, and MUST proceed without one otherwise.
 *       Persistence is deferred to a subsequent {@code REQUIRED} call (typically on a returned
 *       builder).
 *   <li><b>{@code NOT_SUPPORTED}</b> — the operation MUST NOT participate in any transaction. The
 *       operation is purely in-memory and MUST NOT be rolled back by a surrounding transaction.
 * </ul>
 *
 * <p>For implementations backed by a non-JTA {@code JobStore} (for example, document stores without
 * XA support), the store's own atomicity guarantee substitutes for JTA {@code REQUIRED} on its
 * compound operations. Such implementations MUST document which operations are atomic at the store
 * level and which are best-effort.
 *
 * <h2>Security context capture</h2>
 *
 * <p>Implementations MUST capture the caller principal at job creation when a container security
 * context is available and store it on the job entity. When no security context is active or the
 * context provides no authenticated principal, implementations MUST store null. The captured
 * principal MUST be immutable once set — subsequent job mutations (status transitions, retries,
 * rescheduling) MUST NOT overwrite the original capture.
 *
 * <p>No authorization enforcement is performed on job submission, cancellation, pausing,
 * resumption, or retry. The captured principal is exposed on the entity for audit and for
 * downstream consumers that wish to build their own authorization policy; this specification does
 * not prescribe one.
 *
 * @see jakarta.transaction.Transactional
 */
public interface JobSchedulerService {

  /**
   * Starts a fluent builder for a job to execute immediately (zero delay). The returned builder is
   * a factory; no persistence occurs until the builder's {@code submit()} method is invoked.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}. Implementations MUST NOT open a transaction
   * on the builder-creation call. The builder's terminal {@code submit()} call is {@code REQUIRED}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxSupportsContract
   */
  JobBuilder enqueue(SerializableCheckedRunnable task);

  /**
   * Submits a job for immediate execution and returns its handle.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Implementations MUST persist the job within
   * a transaction.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxEnqueueContract
   */
  JobHandle enqueueNow(SerializableCheckedRunnable task);

  /**
   * Starts a fluent builder for a job to execute after the specified delay. The returned builder is
   * a factory; no persistence occurs until {@code submit()} is invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxSupportsContract
   */
  JobBuilder schedule(Duration delay, SerializableCheckedRunnable task);

  /**
   * Starts a fluent builder for a batch of related jobs. No persistence occurs until {@code
   * submit()} is invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxSupportsContract
   */
  BatchBuilder enqueueBatch(String name);

  /**
   * Starts a fluent builder for a streaming batch. No persistence occurs until {@code submit()} is
   * invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxSupportsContract
   */
  <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name);

  /**
   * Starts a fluent builder for a recurring job. No persistence occurs until {@code submit()} is
   * invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxSupportsContract
   */
  RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task);

  /**
   * Replaces an existing job with a new one.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. The existence check, the submission of the
   * replacement, and the cancellation of the old job MUST execute within a single transaction.
   *
   * @param jobId UUIDv7 job id of the job to replace
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  JobHandle replace(
      UUID jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts);

  /**
   * Cancels a job by its ID.
   *
   * <p>If the job is PENDING, it transitions directly to CANCELED. If the job is RUNNING, it
   * transitions to CANCELED and the executor should check status before committing results. Jobs in
   * terminal states (SUCCEEDED, FAILED, CANCELED) cannot be canceled.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was successfully canceled, false if the job was not found or already in
   *     a terminal state
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  boolean cancelJob(UUID jobId);

  /**
   * Registers a programmatic event listener that receives all scheduler events.
   *
   * <p>For type-safe event observation, use CDI {@code @Observes} with specific event types
   * instead. This method is intended for non-CDI contexts or when receiving all events is desired.
   *
   * <p><b>Synchronous dispatch — latency warning.</b> Listeners are invoked synchronously on the
   * publishing thread, which is typically the job execution thread. A slow listener creates
   * unbounded latency on the job hot path and can stall the scheduler. This is intentional — event
   * publication participates in the same transaction as the state change it announces — but it
   * means any listener that does heavyweight work (I/O, network calls, cross-system notifications)
   * MUST offload to its own thread pool. For CDI observers of the same events, prefer
   * {@code @ObservesAsync} when the observer does not need to participate in the source
   * transaction.
   *
   * <p>Event types delivered (all extend {@link
   * run.ratchet.api.event.AbstractJobSchedulerEvent}):
   *
   * <ul>
   *   <li><b>Job lifecycle:</b> {@code JobStartedEvent}, {@code JobCompletedEvent}, {@code
   *       JobFailedEvent}, {@code JobCancellingEvent}, {@code JobCancelledEvent}, {@code
   *       JobPausedEvent}, {@code JobResumedEvent}, {@code JobRetryingEvent}
   *   <li><b>Batch:</b> {@code BatchCompletingEvent}, {@code BatchCompletedEvent}
   *   <li><b>Chain/Workflow:</b> {@code ChainStartedEvent}, {@code ChainCompletedEvent}, {@code
   *       ChainFailedEvent}, {@code WorkflowBranchTriggeredEvent}
   *   <li><b>Observability:</b> {@code JobDlqEvent}, {@code PerformanceMetricsEvent}
   * </ul>
   *
   * <p><b>Transaction attribute:</b> {@code NOT_SUPPORTED}. Listener registration is an in-memory
   * operation and MUST NOT participate in a transaction.
   *
   * @param listener a consumer that receives all scheduler events
   * @see run.ratchet.tck.jakarta.AbstractTxNotSupportedContract
   */
  void addEventListener(Consumer<Object> listener);

  /**
   * Removes a previously registered event listener.
   *
   * <p><b>Transaction attribute:</b> {@code NOT_SUPPORTED}.
   *
   * @see run.ratchet.tck.jakarta.AbstractTxNotSupportedContract
   */
  void removeEventListener(Consumer<Object> listener);

  /**
   * Pauses a job, preventing it from being picked up for execution.
   *
   * <p>Only PENDING or FAILED jobs can be paused. The job's previous status is recorded so it can
   * be restored on resume. Jobs in RUNNING state cannot be paused (cancel them instead).
   *
   * <p>Idempotent: pausing an already-PAUSED job returns {@code true} without error.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was paused or was already paused, false if the job was not found or in
   *     an incompatible state (RUNNING, SUCCEEDED, CANCELED)
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  boolean pauseJob(UUID jobId);

  /**
   * Resumes a paused job, making it eligible for execution again.
   *
   * <p>The job returns to the status it had before being paused. Resuming a previously PENDING job
   * makes it eligible for polling again. Resuming a previously FAILED job restores it to FAILED
   * without retrying it.
   *
   * <p>Idempotent: resuming a non-paused job returns {@code false} without error.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was resumed, false if the job was not found or not in PAUSED state
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  boolean resumeJob(UUID jobId);

  /**
   * Retries a failed job by resetting it to PENDING status.
   *
   * <p>This is the primary mechanism for manual retry of jobs in the Dead Letter Queue. The job's
   * attempt counter is reset to 0, error information is cleared, and scheduled time is set to now
   * so the job becomes immediately eligible for execution.
   *
   * <p>Only FAILED jobs can be retried. Jobs in other states return {@code false}.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was successfully reset to PENDING, false if not found or not FAILED
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  boolean retryJob(UUID jobId);

  /**
   * Cancels all recurring jobs associated with the specified tag.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @return the number of jobs canceled
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  int cancelRecurringJobsByTag(String tag);

  /**
   * Cancels the active recurring job with the specified business key.
   *
   * <p>This is the primary mechanism for replacing a recurring job definition during redeployment.
   * Only jobs in active states (PENDING, RUNNING, PAUSED) with matching business key and recurring
   * job type are affected.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @return the number of jobs canceled (0 or 1, since business keys are active-unique)
   * @see run.ratchet.tck.jakarta.AbstractTxRequiredContract
   */
  int cancelRecurringJobByBusinessKey(String businessKey);
}
