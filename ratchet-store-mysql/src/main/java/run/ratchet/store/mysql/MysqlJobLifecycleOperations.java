package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import java.time.Instant;
import java.util.Set;

final class MysqlJobLifecycleOperations
    implements JobBatchStatusStore, JobRetryStore, JobTerminalStore, JobPauseStore {

  private final MysqlJobStatusTransitions transitions;
  private final MysqlJobTerminalOperations terminals;
  private final MysqlJobRecurringAndResetOperations recurring;

  MysqlJobLifecycleOperations(
      MysqlStoreContext ctx,
      MysqlBusinessKeyReservations reservations,
      MysqlBatchOperations batches) {
    this.transitions = new MysqlJobStatusTransitions(ctx);
    this.terminals = new MysqlJobTerminalOperations(ctx, reservations, batches);
    this.recurring = new MysqlJobRecurringAndResetOperations(ctx, reservations);
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    terminals.updateJobStatus(id, status, errorMessage);
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    return terminals.compareAndSwapStatus(id, expected, newStatus, error);
  }

  @Override
  public int incrementRetryAttempt(long id) {
    return terminals.incrementRetryAttempt(id);
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    return transitions.tryPickUpJob(id, nodeId);
  }

  @Override
  public boolean markJobSucceeded(
      long id,
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
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return terminals.markJobSucceededMinimal(id, start, end, durationMs, queueWaitMs);
  }

  @Override
  public boolean markJobSucceededAndUpdateBatch(
      long jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      long batchId) {
    return terminals.markJobSucceededAndUpdateBatch(
        jobId, resultJson, resultType, start, end, durationMs, queueWaitMs, batchId);
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    return terminals.scheduleJobRetry(id, error, newScheduledTime, attempts);
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    return terminals.markJobFailedTerminal(id, terminalError, totalAttempts);
  }

  @Override
  public boolean cancelJob(long id) {
    return terminals.cancelJob(id);
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
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
  public boolean resetFailedToPending(long id) {
    return terminals.resetFailedToPending(id);
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    return transitions.transitionToPaused(id, expected);
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    return transitions.transitionFromPaused(id, target);
  }

  @Override
  public boolean pauseRecurring(long id) {
    return recurring.pauseRecurring(id);
  }

  @Override
  public boolean resumeRecurring(long id) {
    return recurring.resumeRecurring(id);
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    return transitions.transitionFromPausedAtomic(id);
  }
}
