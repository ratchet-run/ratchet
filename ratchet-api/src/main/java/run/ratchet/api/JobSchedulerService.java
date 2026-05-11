package run.ratchet.api;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
 * <p>Authorization is delegated to the {@link run.ratchet.spi.JobAuthorizationPolicy} SPI. The
 * default reference implementation ({@code PermitAllJobAuthorizationPolicy}) permits all
 * operations. Integrators override via a CDI {@code @Alternative @Priority(APPLICATION)} bean.
 * Note: {@code cancelJobsByTag}, {@code cancelRecurringJobsByTag}, and {@code
 * cancelRecurringJobByBusinessKey} are not subject to per-job authorization; use {@link
 * #cancelJob(UUID)} for authorization-gated single-job cancellation.
 *
 * <p>The RI verifies these contracts through its Jakarta transaction TCK tests.
 */
public interface JobSchedulerService {

  /**
   * Starts a fluent builder for a job to execute immediately (zero delay). The returned builder is
   * a factory; no persistence occurs until the builder's {@code submit()} method is invoked.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}. Implementations MUST NOT open a transaction
   * on the builder-creation call. The builder's terminal {@code submit()} call is {@code REQUIRED}.
   */
  JobBuilder enqueue(SerializableCheckedRunnable task);

  /**
   * Submits a job for immediate execution and returns its handle. Equivalent to {@code
   * enqueue(task).immediate().submit()}.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Implementations MUST persist the job within
   * a transaction.
   */
  default JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return enqueue(task).immediate().submit();
  }

  /**
   * Starts a fluent builder for a job to execute after the specified delay. The returned builder is
   * a factory; no persistence occurs until {@code submit()} is invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   */
  JobBuilder schedule(Duration delay, SerializableCheckedRunnable task);

  /**
   * Starts a fluent builder for a batch of related jobs. No persistence occurs until {@code
   * submit()} is invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   */
  BatchBuilder enqueueBatch(String name);

  /**
   * Starts a fluent builder for a streaming batch. No persistence occurs until {@code submit()} is
   * invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   */
  <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name);

  /**
   * Starts a fluent builder for a recurring job. No persistence occurs until {@code submit()} is
   * invoked on the builder.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   */
  RecurringJobBuilder scheduleRecurring(String cron, ZoneId zone, SerializableCheckedRunnable task);

  /**
   * Convenience overload that schedules a recurring job in UTC. Equivalent to {@code
   * scheduleRecurring(cron, ZoneOffset.UTC, task)}.
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}.
   */
  default RecurringJobBuilder scheduleRecurringUtc(String cron, SerializableCheckedRunnable task) {
    return scheduleRecurring(cron, ZoneOffset.UTC, task);
  }

  /**
   * Replaces an existing job with a new one.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. The existence check, the submission of the
   * replacement, and the cancellation of the old job MUST execute within a single transaction.
   *
   * @param jobId UUIDv7 job id of the job to replace
   * @param delay delay before the replacement job becomes eligible to run
   * @param newTask task to execute for the replacement job
   * @param opts optional job options to apply to the replacement; {@code null} uses implementation
   *     defaults
   * @return handle for the newly submitted replacement job
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
   * <p>Event types delivered:
   *
   * <ul>
   *   <li><b>Job lifecycle:</b> {@code JobStartedEvent}, {@code JobCompletedEvent}, {@code
   *       JobFailedEvent}, {@code JobCancelledEvent}, {@code JobsBulkCancelledEvent}, {@code
   *       JobPausedEvent}, {@code JobResumedEvent}, {@code JobRetryingEvent}
   *   <li><b>Batch:</b> {@code BatchCompletingEvent}, {@code BatchCompletedEvent}
   *   <li><b>Chain/Workflow:</b> {@code ChainStartedEvent}, {@code ChainCompletedEvent}, {@code
   *       ChainFailedEvent}, {@code WorkflowBranchTriggeredEvent}
   *   <li><b>Signals:</b> {@code JobSignalWaitingEvent}, {@code JobSignaledEvent}, {@code
   *       JobsBulkSignaledEvent}, {@code JobSignalTimedOutEvent}
   *   <li><b>Observability:</b> {@code JobDlqEvent}, {@code PerformanceMetricsEvent}
   * </ul>
   *
   * <p><b>Transaction attribute:</b> {@code NOT_SUPPORTED}. Listener registration is an in-memory
   * operation and MUST NOT participate in a transaction.
   *
   * @param listener a consumer that receives all scheduler events
   */
  void addEventListener(Consumer<Object> listener);

  /**
   * Removes a previously registered event listener.
   *
   * <p><b>Transaction attribute:</b> {@code NOT_SUPPORTED}.
   */
  void removeEventListener(Consumer<Object> listener);

  /**
   * Pauses a job, preventing it from being picked up for execution.
   *
   * <p>Only PENDING jobs can be paused. The job's previous status is recorded so it can be restored
   * on resume. Jobs in RUNNING, WAITING, or terminal states cannot be paused.
   *
   * <p>Idempotent: pausing an already-PAUSED job returns {@code true} without error.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was paused or was already paused, false if the job was not found or in
   *     an incompatible state (RUNNING, WAITING, SUCCEEDED, FAILED, CANCELED)
   */
  boolean pauseJob(UUID jobId);

  /**
   * Resumes a paused job, making it eligible for execution again.
   *
   * <p>Resuming a previously PENDING job makes it eligible for polling again.
   *
   * <p>Idempotent: resuming a non-paused job returns {@code false} without error.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id
   * @return true if the job was resumed, false if the job was not found or not in PAUSED state
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
   */
  boolean retryJob(UUID jobId);

  /**
   * Delivers a signal to the specific WAITING job identified by {@code jobId}, transitioning it to
   * PENDING so it can be picked up for execution.
   *
   * <p>Idempotent: if the job is already in a non-WAITING state (including terminal states), this
   * method returns {@code 0} without modifying the job.
   *
   * <p>The signal payload is serialized and stored on the job entity; it is accessible to the
   * executing task via {@link JobContext#signalPayload(Class)}.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id of the WAITING job
   * @param payload optional payload to pass to the executing job; may be null
   * @return 1 if the job was unblocked, 0 if the job was not found, not in WAITING state, or signal
   *     support is not configured
   */
  int deliverSignal(UUID jobId, Serializable payload);

  /**
   * Delivers a structured approval/rejection decision to the specific WAITING job identified by
   * {@code jobId}, transitioning it to PENDING so job code can consume the decision through {@link
   * JobContext#signalPayload(Class)}.
   *
   * <p>Idempotent: if the job is already in a non-WAITING state (including terminal states), this
   * method returns {@code 0} without modifying the job.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param jobId UUIDv7 job id of the WAITING job
   * @param decision decision payload; must not be null
   * @return 1 if the job was unblocked, 0 if the job was not found, not in WAITING state, or signal
   *     support is not configured
   */
  int deliverSignal(UUID jobId, SignalDecision decision);

  /**
   * Delivers a signal to all WAITING jobs whose {@code signalKey} matches, transitioning each to
   * PENDING.
   *
   * <p>This is an atomic bulk operation: stores MUST implement it as a single UPDATE WHERE {@code
   * signal_key = ? AND status = 'WAITING'} (SQL) or equivalent {@code updateMany} within a session
   * transaction (MongoDB) to prevent duplicate-delivery races.
   *
   * <p>Idempotent: jobs already past WAITING are not affected and do not count toward the return
   * value.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param signalKey the named signal to broadcast
   * @param payload optional payload delivered to every unblocked job; may be null
   * @return the number of jobs transitioned from WAITING to PENDING, or 0 if no jobs were waiting
   *     or signal support is not configured
   */
  int deliverSignal(String signalKey, Serializable payload);

  /**
   * Delivers a structured approval/rejection decision to all WAITING jobs whose {@code signalKey}
   * matches, transitioning each to PENDING.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param signalKey the named signal to broadcast
   * @param decision decision payload; must not be null
   * @return the number of jobs transitioned from WAITING to PENDING, or 0 if no jobs were waiting
   *     or signal support is not configured
   */
  int deliverSignal(String signalKey, SignalDecision decision);

  /**
   * Cancels all active non-recurring jobs associated with the specified tag.
   *
   * <p>Affects only jobs in {@link JobStatus#PENDING}, {@link JobStatus#PAUSED}, and {@link
   * JobStatus#WAITING} states. Jobs in {@link JobStatus#RUNNING} state are not affected; the
   * executor observes their natural termination. Recurring jobs are not affected — use {@link
   * #cancelRecurringJobsByTag(String)} for that.
   *
   * <p>This is a coordination primitive (kill-switch / batch teardown) rather than preemption.
   * Implementations MUST execute as a single bulk operation per call (one statement per affected
   * table on SQL stores, {@code updateMany} on document stores) — not as a per-row loop.
   *
   * <p>Implementations MUST publish exactly one {@link
   * run.ratchet.api.event.JobsBulkCancelledEvent} per call when the returned count is greater than
   * zero, with the matching {@code (tag, count)}. No per-job {@code JobCancelledEvent} is fired for
   * jobs cancelled by this method.
   *
   * <p>Not subject to per-job {@link run.ratchet.spi.JobAuthorizationPolicy} authorization; use
   * {@link #cancelJob(UUID)} for authorization-gated single-job cancellation.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @param tag the tag identifying jobs to cancel
   * @return the number of jobs cancelled
   */
  int cancelJobsByTag(String tag);

  /**
   * Cancels all recurring jobs associated with the specified tag.
   *
   * <p>Implementations MUST execute as a single bulk operation per call.
   *
   * <p>Implementations MUST publish exactly one {@link
   * run.ratchet.api.event.JobsBulkCancelledEvent} per call when the returned count is greater than
   * zero, with the matching {@code (tag, count)}.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   *
   * @return the number of jobs canceled
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
   */
  int cancelRecurringJobByBusinessKey(String businessKey);
}
