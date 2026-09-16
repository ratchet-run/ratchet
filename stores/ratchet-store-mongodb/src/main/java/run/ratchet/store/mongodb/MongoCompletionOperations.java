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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static run.ratchet.store.mongodb.MongoFieldNames.*;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;

/** All durable consequences of one terminal transition share this explicit session. */
final class MongoCompletionOperations {
  private MongoCompletionOperations() {}

  static JobCompletionResult commit(
      MongoStoreContext ctx, MongoBusinessKeyReservations reservations, JobCompletionPlan plan) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(() -> apply(ctx, reservations, session, plan));
    }
  }

  private static JobCompletionResult apply(
      MongoStoreContext ctx,
      MongoBusinessKeyReservations reservations,
      ClientSession session,
      JobCompletionPlan plan) {
    Instant now = Instant.now();
    long changed =
        ctx.jobs()
            .updateOne(
                session,
                and(eq(ID, plan.jobId()), eq(STATUS, plan.expectedStatus().name())),
                combine(
                    set(STATUS, plan.terminalStatus().name()),
                    set(JOB_RESULT, plan.resultJson()),
                    set(RESULT_TYPE, plan.resultType()),
                    set(LAST_ERROR, plan.errorMessage()),
                    set(ATTEMPTS, plan.attempts()),
                    set(EXECUTION_START_TIME, DocumentMapper.toDate(plan.start())),
                    set(EXECUTION_END_TIME, DocumentMapper.toDate(plan.end())),
                    set(EXECUTION_DURATION_MS, plan.durationMs()),
                    set(QUEUE_WAIT_MS, plan.queueWaitMs()),
                    set(TERMINATED_AT, DocumentMapper.toDate(now)),
                    set(UPDATED_AT, DocumentMapper.toDate(now)),
                    inc(VERSION, 1)))
            .getMatchedCount();
    if (changed == 0) return JobCompletionResult.notCommitted();
    reservations.releaseByOwner(session, plan.jobId());

    if (plan.completedBatch() != null) {
      var guard = plan.completedBatch();
      changed =
          ctx.batches()
              .updateOne(
                  session,
                  and(
                      eq(ID, plan.jobId()),
                      eq(COMPLETION_PROCESSED, false),
                      eq(TOTAL_ITEMS, guard.totalItems()),
                      eq(COMPLETED_ITEMS, guard.completedItems()),
                      eq(FAILED_ITEMS, guard.failedItems())),
                  set(COMPLETION_PROCESSED, true))
              .getMatchedCount();
      requireCurrent(changed, "batch completion");
      Document metrics = ctx.batchMetrics().find(session, eq(ID, plan.jobId())).first();
      if (metrics != null && metrics.getDate(COMPLETED_AT) == null) {
        var started = metrics.getDate(STARTED_AT);
        Long total = started == null ? null : Duration.between(started.toInstant(), now).toMillis();
        Number child = (Number) metrics.get(CHILD_EXECUTION_MS);
        Long overhead = total == null || child == null ? null : total - child.longValue();
        ctx.batchMetrics()
            .updateOne(
                session,
                eq(ID, plan.jobId()),
                combine(
                    set(COMPLETED_AT, DocumentMapper.toDate(now)),
                    set(TOTAL_DURATION_MS, total),
                    set(OVERHEAD_MS, overhead)));
      }
    }

    for (var dep : plan.dependencies()) {
      List<Bson> changes = new ArrayList<>();
      changes.add(set(STATUS, dep.status().name()));
      changes.add(set(SCHEDULED_TIME, DocumentMapper.toDate(dep.scheduledTime())));
      changes.add(set(JOB_TYPE, dep.jobType().name()));
      changes.add(set(UPDATED_AT, DocumentMapper.toDate(now)));
      changes.add(inc(VERSION, 1));
      if (dep.status() == JobStatus.CANCELED)
        changes.add(set(TERMINATED_AT, DocumentMapper.toDate(now)));
      changed =
          ctx.jobs()
              .updateOne(
                  session,
                  and(
                      eq(ID, dep.jobId()), eq(STATUS, dep.expectedStatus().name()),
                      eq(VERSION, dep.expectedVersion()),
                          eq(SCHEDULED_TIME, DocumentMapper.toDate(dep.expectedScheduledTime()))),
                  combine(changes))
              .getMatchedCount();
      requireCurrent(changed, "dependent transition");
      if (dep.status() == JobStatus.CANCELED) reservations.releaseByOwner(session, dep.jobId());
    }

    BatchProgress progress = null;
    if (plan.batchId() != null) {
      boolean success = plan.terminalStatus() == JobStatus.SUCCEEDED;
      Document batch =
          ctx.batches()
              .findOneAndUpdate(
                  session,
                  eq(ID, plan.batchId()),
                  inc(success ? COMPLETED_ITEMS : FAILED_ITEMS, 1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
      if (batch == null) throw new IllegalStateException("Batch not found: " + plan.batchId());
      progress = DocumentMapper.toBatchProgress(batch, plan.batchId());
      if (success && plan.durationMs() != null) {
        ctx.batchMetrics()
            .updateOne(
                session,
                eq(ID, plan.batchId()),
                combine(inc(CHILD_EXECUTION_MS, plan.durationMs()), inc(SUCCESS_COUNT, 1)));
      }
    }
    return new JobCompletionResult(true, progress);
  }

  private static void requireCurrent(long changed, String mutation) {
    if (changed == 0)
      throw new RatchetTransientStoreException("Stale " + mutation + "; replan completion");
  }
}
