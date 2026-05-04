package run.ratchet.store.postgresql;

import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.store.spi.JobTerminalStore;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

final class PostgresqlJobLifecycleOperations
    implements JobStatusStore, JobTerminalStore, JobRetryStore, JobPauseStore {

  private final PostgresqlJobStatusTransitions transitions;
  private final PostgresqlJobTerminalOperations terminals;
  private final PostgresqlJobRecurringAndResetOperations recurring;

  PostgresqlJobLifecycleOperations(
      PostgresqlStoreContext ctx,
      PostgresqlBusinessKeyReservations reservations,
      PostgresqlBatchOperations batches) {
    this.transitions = new PostgresqlJobStatusTransitions(ctx);
    this.terminals = new PostgresqlJobTerminalOperations(ctx, reservations, batches);
    this.recurring = new PostgresqlJobRecurringAndResetOperations(ctx, reservations);
  }

  @Override
  public void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    terminals.updateJobStatus(id, status, errorMessage);
  }

  @Override
  public boolean compareAndSwapStatus(
      UUID id, JobStatus expected, JobStatus newStatus, String error) {
    return terminals.compareAndSwapStatus(id, expected, newStatus, error);
  }

  @Override
  public int incrementRetryAttempt(UUID id) {
    return terminals.incrementRetryAttempt(id);
  }

  @Override
  public boolean tryPickUpJob(UUID id, String nodeId) {
    return transitions.tryPickUpJob(id, nodeId);
  }

  @Override
  public boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    return terminals.markJobSucceeded(
        id, resultJson, resultType, start, end, durationMs, queueWaitMs);
  }

  @Override
  public boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return terminals.markJobSucceededMinimal(id, start, end, durationMs, queueWaitMs);
  }

  @Override
  public boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId) {
    return terminals.markJobSucceededAndUpdateBatch(
        jobId, resultJson, resultType, start, end, durationMs, queueWaitMs, batchId);
  }

  @Override
  public boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    return terminals.scheduleJobRetry(id, error, newScheduledTime, attempts);
  }

  @Override
  public boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return terminals.markJobFailedTerminal(id, terminalError, totalAttempts);
  }

  @Override
  public boolean cancelJob(UUID id) {
    return terminals.cancelJob(id);
  }

  @Override
  public boolean resetRunningJob(UUID id, String nodeId) {
    return recurring.resetRunningJob(id, nodeId);
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return recurring.resetRunningJobs(nodeId);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return recurring.cancelRecurringJobsByTag(tag);
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return recurring.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return recurring.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return terminals.resetFailedToPending(id);
  }

  @Override
  public boolean transitionToPaused(UUID id, JobStatus expected) {
    return transitions.transitionToPaused(id, expected);
  }

  @Override
  public boolean transitionFromPaused(UUID id, JobStatus target) {
    return transitions.transitionFromPaused(id, target);
  }

  @Override
  public boolean pauseRecurring(UUID id) {
    return recurring.pauseRecurring(id);
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    return recurring.resumeRecurring(id);
  }

  @Override
  public JobStatus transitionFromPausedAtomic(UUID id) {
    return transitions.transitionFromPausedAtomic(id);
  }
}
