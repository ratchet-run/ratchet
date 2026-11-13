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
package run.ratchet.store.util;

import jakarta.persistence.EntityManager;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TerminalOutcome;

/** Applies a completion within the calling SQL store's REQUIRED transaction. */
public final class SqlJobCompletion {
  private SqlJobCompletion() {}

  public static JobCompletionResult commit(
      EntityManager em,
      JobStore store,
      BatchStore batches,
      JobCompletionPlan plan,
      boolean sqlServer,
      Function<UUID, Object> uuid) {
    return commit(
        em,
        store,
        batches,
        plan,
        sqlServer,
        uuid,
        dependency -> {
          if (dependency.payload() != null)
            throw new UnsupportedOperationException("Payload rewrite requires a dialect binding");
        });
  }

  public static JobCompletionResult commit(
      EntityManager em,
      JobStore store,
      BatchStore batches,
      JobCompletionPlan plan,
      boolean sqlServer,
      Function<UUID, Object> uuid,
      Consumer<JobCompletionPlan.DependencyTransition> payloadWriter) {
    // Flush managed state before locking and reading native projections. Lock all job rows in a
    // stable order so competing completions of overlapping dependency sets cannot invert locks.
    em.flush();
    List<UUID> ids = new ArrayList<>();
    ids.add(plan.jobId());
    plan.dependencies().forEach(d -> ids.add(d.jobId()));
    var locked = new HashMap<UUID, Object[]>();
    ids.stream()
        .distinct()
        .sorted()
        .forEach(
            id -> {
              List<?> rows =
                  em.createNativeQuery(
                          lockSql(
                              "scheduler_job_queue",
                              "job_id",
                              "status, version, scheduled_time, attempts, claim_seq",
                              sqlServer))
                      .setParameter(1, uuid.apply(id))
                      .getResultList();
              if (!rows.isEmpty()) locked.put(id, (Object[]) rows.get(0));
            });
    Object[] primary = locked.get(plan.jobId());
    if (primary == null || !plan.expectedStatus().name().equals(primary[0]))
      return JobCompletionResult.notCommitted();
    if (plan.expectedClaimSeq() != null
        && ((Number) primary[4]).longValue() != plan.expectedClaimSeq())
      return JobCompletionResult.notCommitted();
    for (var dependency : plan.dependencies()) {
      Object[] row = locked.get(dependency.jobId());
      if (row == null
          || !dependency.expectedStatus().name().equals(row[0])
          || !Objects.equals(
              dependency.expectedVersion(), row[1] == null ? null : ((Number) row[1]).intValue())
          || !Objects.equals(dependency.expectedScheduledTime(), RowValues.instantOrNull(row[2])))
        throw stale("Dependency changed", dependency.jobId());
    }
    if (plan.completedBatch() != null) {
      List<?> rows =
          em.createNativeQuery(
                  lockSql(
                      "scheduler_batch",
                      "batch_id",
                      "total_items, completed_items, failed_items, completion_processed",
                      sqlServer))
              .setParameter(1, uuid.apply(plan.jobId()))
              .getResultList();
      if (rows.isEmpty()) throw stale("Batch missing", plan.jobId());
      Object[] row = (Object[]) rows.get(0);
      var expected = plan.completedBatch();
      boolean processed =
          Boolean.TRUE.equals(row[3]) || row[3] instanceof Number n && n.intValue() != 0;
      if (((Number) row[0]).intValue() != expected.totalItems()
          || ((Number) row[1]).intValue() != expected.completedItems()
          || ((Number) row[2]).intValue() != expected.failedItems()
          || processed) throw stale("Batch changed", plan.jobId());
    }

    // Synthetic parents have no worker. Their transient RUNNING state stays inside this commit.
    if (plan.expectedStatus() == JobStatus.PENDING && plan.completedBatch() != null) {
      if (!store.tryPickUpJob(plan.jobId(), "batch-completion"))
        throw stale("Batch claim lost", plan.jobId());
    }
    // Failure APIs historically accept RUNNING only. WAITING timeouts use the guarded terminal
    // CAS, after setting the attempts in the same transaction so terminal history remains exact.
    // The row is already locked. Most completions retain its attempt count, so avoid an
    // extra write while preserving plans that deliberately change terminal history.
    if (((Number) primary[3]).intValue() != plan.attempts()) {
      em.createNativeQuery("UPDATE scheduler_job_queue SET attempts = ? WHERE job_id = ?")
          .setParameter(1, plan.attempts())
          .setParameter(2, uuid.apply(plan.jobId()))
          .executeUpdate();
    }
    boolean transitioned =
        switch (plan.terminalStatus()) {
          case SUCCEEDED ->
              store.markJobSucceeded(
                  plan.jobId(),
                  plan.resultJson(),
                  plan.resultType(),
                  plan.start(),
                  plan.end(),
                  plan.durationMs(),
                  plan.queueWaitMs());
          case FAILED ->
              plan.expectedStatus() == JobStatus.WAITING
                  ? store.compareAndSwapStatus(
                      plan.jobId(), JobStatus.WAITING, JobStatus.FAILED, plan.errorMessage())
                  : store.markJobFailedTerminal(
                      plan.jobId(), plan.errorMessage(), plan.attempts(), plan.expectedClaimSeq());
          case CANCELED -> store.cancelJob(plan.jobId());
          default -> throw new IllegalArgumentException("Not a terminal status");
        };
    if (!transitioned) throw stale("Terminal transition lost", plan.jobId());
    for (var dependency : plan.dependencies()) {
      if (dependency.status() == JobStatus.CANCELED) {
        if (!store.cancelJob(dependency.jobId()))
          throw stale("Dependency cancel lost", dependency.jobId());
      } else {
        if (dependency.payload() != null) payloadWriter.accept(dependency);
        int changed =
            em.createNativeQuery(
                    "UPDATE scheduler_job_queue SET status = ?, scheduled_time = ?, job_type = ?,"
                        + " version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?")
                .setParameter(1, dependency.status().name())
                .setParameter(
                    2,
                    dependency.scheduledTime() == null
                        ? null
                        : Timestamp.from(dependency.scheduledTime()))
                .setParameter(3, dependency.jobType().name())
                .setParameter(4, uuid.apply(dependency.jobId()))
                .executeUpdate();
        if (changed != 1) throw stale("Dependency update lost", dependency.jobId());
        em.createNativeQuery("UPDATE scheduler_job SET job_type = ? WHERE job_id = ?")
            .setParameter(1, dependency.jobType().name())
            .setParameter(2, uuid.apply(dependency.jobId()))
            .executeUpdate();
      }
    }
    BatchProgress progress = null;
    if (plan.batchId() != null && plan.terminalStatus() == JobStatus.CANCELED) {
      // cancelJob already counted the child as failed; report the counters it left behind.
      var batch =
          batches
              .findBatchById(plan.batchId())
              .orElseThrow(() -> stale("Batch missing", plan.batchId()));
      progress =
          new BatchProgress(
              plan.batchId(),
              batch.getTotalItems(),
              batch.getCompletedItems(),
              batch.getFailedItems(),
              batch.getProgressHook());
    } else if (plan.batchId() != null) {
      progress =
          plan.terminalStatus() == JobStatus.SUCCEEDED
              ? batches.incrementCompletedAtomic(plan.batchId())
              : batches.incrementFailedAtomic(plan.batchId());
      if (progress == null) throw stale("Batch missing", plan.batchId());
      if (plan.terminalStatus() == JobStatus.SUCCEEDED && plan.durationMs() != null)
        batches.addChildExecutionTime(plan.batchId(), plan.durationMs());
    }
    if (plan.completedBatch() != null) {
      if (!batches.markBatchCompleteIfReady(plan.jobId()))
        throw stale("Batch completion lost", plan.jobId());
      batches.finalizeBatchMetrics(plan.jobId());
    }
    for (var event : plan.outboxEvents()) SqlEventOutbox.insert(em, event, sqlServer, uuid);
    return new JobCompletionResult(true, progress);
  }

  /** Resolves a lost commit acknowledgement using the same locks as completion. */
  public static TerminalOutcome terminalOutcome(
      EntityManager em, UUID jobId, long claimSeq, boolean sqlServer, Function<UUID, Object> uuid) {
    List<?> hot =
        em.createNativeQuery(
                lockSql("scheduler_job_queue", "job_id", "status, claim_seq", sqlServer))
            .setParameter(1, uuid.apply(jobId))
            .getResultList();
    List<?> cold =
        em.createNativeQuery(
                lockSql("scheduler_job", "job_id", "terminal_status, claim_seq", sqlServer))
            .setParameter(1, uuid.apply(jobId))
            .getResultList();
    if (!hot.isEmpty()) {
      Object[] row = (Object[]) hot.get(0);
      return "RUNNING".equals(row[0]) && ((Number) row[1]).longValue() == claimSeq
          ? TerminalOutcome.ROLLED_BACK
          : TerminalOutcome.SUPERSEDED;
    }
    if (!cold.isEmpty()) {
      Object[] row = (Object[]) cold.get(0);
      if ("SUCCEEDED".equals(row[0]) && ((Number) row[1]).longValue() == claimSeq) {
        return TerminalOutcome.COMMITTED;
      }
      if ("CANCELED".equals(row[0])) return TerminalOutcome.CANCELED;
    }
    return TerminalOutcome.SUPERSEDED;
  }

  private static String lockSql(String table, String id, String columns, boolean sqlServer) {
    return "SELECT "
        + columns
        + " FROM "
        + table
        + (sqlServer ? " WITH (UPDLOCK, ROWLOCK)" : "")
        + " WHERE "
        + id
        + " = ?"
        + (sqlServer ? "" : " FOR UPDATE");
  }

  private static RatchetTransientStoreException stale(String message, UUID id) {
    return new RatchetTransientStoreException(message + ": " + id);
  }
}
