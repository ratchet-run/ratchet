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
package run.ratchet.store.postgresql;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.util.BulkRetryFilters;

final class PostgresqlJobLifecycleOperations
    implements JobBatchStatusStore, JobTerminalStore, JobRetryStore, JobPauseStore {

  private final PostgresqlJobStatusTransitions transitions;
  private final PostgresqlJobTerminalOperations terminals;
  private final PostgresqlJobRecurringAndResetOperations recurring;
  private final PostgresqlJobQueryOperations query;

  PostgresqlJobLifecycleOperations(
      PostgresqlStoreContext ctx,
      PostgresqlBusinessKeyReservations reservations,
      PostgresqlBatchOperations batches,
      PostgresqlJobQueryOperations query) {
    this.transitions = new PostgresqlJobStatusTransitions(ctx);
    this.terminals = new PostgresqlJobTerminalOperations(ctx, reservations, batches);
    this.recurring = new PostgresqlJobRecurringAndResetOperations(ctx);
    this.query = query;
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
  public int cancelJobsByTag(String tag) {
    return recurring.cancelJobsByTag(tag);
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return terminals.resetFailedToPending(id);
  }

  @Override
  public int resetFailedToPending(JobFilter filter, int limit) {
    JobFilter failed = BulkRetryFilters.normalize(filter, limit);
    if (failed == null) {
      return 0;
    }
    List<UUID> ids = query.searchJobs(failed, limit, 0).stream().map(job -> job.getId()).toList();
    return terminals.resetFailedToPending(ids);
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
  public JobStatus transitionFromPausedAtomic(UUID id) {
    return transitions.transitionFromPausedAtomic(id);
  }
}
