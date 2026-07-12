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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPTS;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_END_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_START_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_RESULT;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.LAST_ERROR;
import static run.ratchet.store.mongodb.MongoFieldNames.PAUSED_FROM_STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.QUEUE_WAIT_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.RESULT_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.util.BulkRetryFilters;
import run.ratchet.store.util.StatusClassifier;

/**
 * Job state-transition operations. Every method targets a specific {@code (id, status)}
 * precondition to stay race-free — Mongo has no row lock, so safety comes from atomic filters plus
 * transactions when a transition also updates a cross-collection invariant. Cross-job side effects
 * (e.g. bumping batch counters on success) are delegated back into {@link MongoBatchOperations}.
 */
final class MongoJobLifecycleOperations
    implements JobBatchStatusStore, JobPauseStore, JobRetryStore, JobTerminalStore {

  private static final Logger log = Logger.getLogger(MongoJobLifecycleOperations.class);

  private final MongoStoreContext ctx;
  private final MongoBatchOperations batches;
  private final MongoBusinessKeyReservations reservations;
  private final MongoJobQueryOperations query;

  MongoJobLifecycleOperations(MongoStoreContext ctx, MongoBatchOperations batches) {
    this(ctx, batches, new MongoBusinessKeyReservations(ctx), new MongoJobQueryOperations(ctx));
  }

  MongoJobLifecycleOperations(
      MongoStoreContext ctx,
      MongoBatchOperations batches,
      MongoBusinessKeyReservations reservations) {
    this(ctx, batches, reservations, new MongoJobQueryOperations(ctx));
  }

  MongoJobLifecycleOperations(
      MongoStoreContext ctx, MongoBatchOperations batches, MongoJobQueryOperations query) {
    this(ctx, batches, new MongoBusinessKeyReservations(ctx), query);
  }

  MongoJobLifecycleOperations(
      MongoStoreContext ctx,
      MongoBatchOperations batches,
      MongoBusinessKeyReservations reservations,
      MongoJobQueryOperations query) {
    this.ctx = ctx;
    this.batches = batches;
    this.reservations = reservations;
    this.query = query;
  }

  @Override
  public void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    // A live target is a plain status set. Terminal targets route through the guarded terminal
    // methods so the picked-by/terminated-at cleanup matches the SQL stores, which never let
    // updateJobStatus flip a row straight to a terminal status without that bookkeeping.
    if (!StatusClassifier.isLiveStatus(status)) {
      switch (status) {
        case CANCELED -> cancelJob(id);
        case FAILED -> markJobFailedTerminal(id, errorMessage, 0);
        case SUCCEEDED -> markJobSucceededMinimal(id, null, null, null, null);
        default -> throw new IllegalArgumentException("Unsupported status target: " + status);
      }
      return;
    }
    runMutation(
        "update_status",
        () -> {
          Instant now = Instant.now();
          transactionalStatusMutation(
              id,
              status,
              session ->
                  ctx.jobs()
                      .updateOne(
                          session,
                          eq(ID, id),
                          combine(
                              set(STATUS, status.name()),
                              set(LAST_ERROR, errorMessage),
                              set(UPDATED_AT, DocumentMapper.toDate(now)),
                              set(TERMINATED_AT, terminalDate(status, now)),
                              inc(VERSION, 1))));
        });
  }

  @Override
  public boolean compareAndSwapStatus(
      UUID id, JobStatus expected, JobStatus newStatus, String error) {
    if (!StatusClassifier.isLiveStatus(expected)) {
      throw new IllegalArgumentException(
          "compareAndSwapStatus expected must be a live status; got " + expected);
    }
    return ctx.timedStoreOperation(
        "compare_and_swap_status",
        () -> {
          Instant now = Instant.now();
          return transactionalStatusMutation(
              id,
              newStatus,
              session -> {
                if ((newStatus == JobStatus.FAILED || newStatus == JobStatus.CANCELED)
                    && expected == JobStatus.RUNNING) {
                  return ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, expected.name())),
                          combine(
                              set(STATUS, newStatus.name()),
                              set(LAST_ERROR, error),
                              set(EXECUTION_START_TIME, DocumentMapper.toDate(now)),
                              set(EXECUTION_END_TIME, DocumentMapper.toDate(now)),
                              set(EXECUTION_DURATION_MS, 0L),
                              set(PICKED_BY, null),
                              set(PICKED_AT, null),
                              set(UPDATED_AT, DocumentMapper.toDate(now)),
                              set(TERMINATED_AT, terminalDate(newStatus, now)),
                              inc(VERSION, 1)));
                }
                return ctx.jobs()
                    .updateOne(
                        session,
                        and(eq(ID, id), eq(STATUS, expected.name())),
                        combine(
                            set(STATUS, newStatus.name()),
                            set(LAST_ERROR, error),
                            set(UPDATED_AT, DocumentMapper.toDate(now)),
                            set(TERMINATED_AT, terminalDate(newStatus, now)),
                            inc(VERSION, 1)));
              });
        },
        updated -> updated ? "updated" : "miss");
  }

  @Override
  public int incrementRetryAttempt(UUID id) {
    return intMutation(
        "increment_retry_attempt",
        () -> {
          Document doc =
              ctx.jobs()
                  .findOneAndUpdate(
                      and(eq(ID, id), in(STATUS, List.of("RUNNING", "WAITING"))),
                      combine(
                          inc(ATTEMPTS, 1),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)),
                      new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
          if (doc == null) {
            log.debugf(
                "Retry attempt was not incremented because job %s is missing or not retryable", id);
            return -1;
          }
          return doc.getInteger(ATTEMPTS);
        });
  }

  @Override
  public boolean tryPickUpJob(UUID id, String nodeId) {
    return booleanMutation(
        "pickup_job",
        () -> {
          Instant now = Instant.now();
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), eq(STATUS, "PENDING")),
                      combine(
                          set(STATUS, "RUNNING"),
                          set(PICKED_BY, nodeId),
                          set(PICKED_AT, DocumentMapper.toDate(now)),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
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
    return ctx.timedStoreOperation(
        "mark_succeeded",
        () -> {
          Instant now = Instant.now();
          return transactionalStatusMutation(
              id,
              JobStatus.SUCCEEDED,
              session ->
                  ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, "RUNNING")),
                          combine(
                              set(STATUS, "SUCCEEDED"),
                              set(JOB_RESULT, resultJson),
                              set(RESULT_TYPE, resultType),
                              set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                              set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                              set(TERMINATED_AT, DocumentMapper.toDate(now)),
                              set(EXECUTION_DURATION_MS, durationMs),
                              set(QUEUE_WAIT_MS, queueWaitMs),
                              set(LAST_ERROR, null),
                              set(UPDATED_AT, DocumentMapper.toDate(now)),
                              inc(VERSION, 1))));
        },
        updated -> updated ? "updated" : "miss");
  }

  @Override
  public boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return ctx.timedStoreOperation(
        "mark_succeeded_minimal",
        () -> {
          Instant now = Instant.now();
          return transactionalStatusMutation(
              id,
              JobStatus.SUCCEEDED,
              session ->
                  ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, "RUNNING")),
                          combine(
                              set(STATUS, "SUCCEEDED"),
                              set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                              set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                              set(TERMINATED_AT, DocumentMapper.toDate(now)),
                              set(EXECUTION_DURATION_MS, durationMs),
                              set(QUEUE_WAIT_MS, queueWaitMs),
                              set(LAST_ERROR, null),
                              set(UPDATED_AT, DocumentMapper.toDate(now)),
                              inc(VERSION, 1))));
        },
        updated -> updated ? "updated" : "miss");
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
    return ctx.timedStoreOperation(
        "mark_succeeded_and_update_batch",
        () -> {
          try (ClientSession session = ctx.startSession()) {
            return session.withTransaction(
                () -> {
                  Instant now = Instant.now();
                  UpdateResult result =
                      ctx.jobs()
                          .updateOne(
                              session,
                              and(eq(ID, jobId), eq(STATUS, "RUNNING")),
                              combine(
                                  set(STATUS, "SUCCEEDED"),
                                  set(JOB_RESULT, resultJson),
                                  set(RESULT_TYPE, resultType),
                                  set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                                  set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                                  set(TERMINATED_AT, DocumentMapper.toDate(now)),
                                  set(EXECUTION_DURATION_MS, durationMs),
                                  set(QUEUE_WAIT_MS, queueWaitMs),
                                  set(LAST_ERROR, null),
                                  set(UPDATED_AT, DocumentMapper.toDate(now)),
                                  inc(VERSION, 1)));
                  if (result.getModifiedCount() == 0) {
                    return false;
                  }
                  reservations.releaseByOwner(session, jobId);
                  batches.incrementCompletedAtomic(session, batchId);
                  return true;
                });
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  @Override
  public boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    return booleanMutation(
        "schedule_retry",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), in(STATUS, List.of("RUNNING", "WAITING"))),
                      combine(
                          set(STATUS, "PENDING"),
                          set(SCHEDULED_TIME, DocumentMapper.toDate(newScheduledTime)),
                          set(ATTEMPTS, attempts),
                          set(LAST_ERROR, error),
                          set(PICKED_BY, null),
                          set(PICKED_AT, null),
                          unset(TERMINATED_AT),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return booleanMutation(
        "mark_failed_terminal",
        () -> {
          Instant now = Instant.now();
          return transactionalStatusMutation(
              id,
              JobStatus.FAILED,
              session ->
                  ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, "RUNNING")),
                          combine(
                              set(STATUS, "FAILED"),
                              set(LAST_ERROR, terminalError),
                              set(ATTEMPTS, totalAttempts),
                              set(EXECUTION_START_TIME, DocumentMapper.toDate(now)),
                              set(EXECUTION_END_TIME, DocumentMapper.toDate(now)),
                              set(EXECUTION_DURATION_MS, 0L),
                              set(PICKED_BY, null),
                              set(PICKED_AT, null),
                              set(UPDATED_AT, DocumentMapper.toDate(now)),
                              set(TERMINATED_AT, DocumentMapper.toDate(now)),
                              inc(VERSION, 1))));
        });
  }

  @Override
  public boolean cancelJob(UUID id) {
    return booleanMutation(
        "cancel_job",
        () -> {
          Instant now = Instant.now();
          return transactionalStatusMutation(
              id,
              JobStatus.CANCELED,
              session -> {
                UpdateResult runningResult =
                    ctx.jobs()
                        .updateOne(
                            session,
                            and(eq(ID, id), eq(STATUS, "RUNNING")),
                            combine(
                                set(STATUS, "CANCELED"),
                                set(EXECUTION_START_TIME, DocumentMapper.toDate(now)),
                                set(EXECUTION_END_TIME, DocumentMapper.toDate(now)),
                                set(EXECUTION_DURATION_MS, 0L),
                                set(PICKED_BY, null),
                                set(PICKED_AT, null),
                                set(UPDATED_AT, DocumentMapper.toDate(now)),
                                set(TERMINATED_AT, DocumentMapper.toDate(now)),
                                inc(VERSION, 1)));
                if (runningResult.getModifiedCount() > 0) {
                  return runningResult;
                }
                return ctx.jobs()
                    .updateOne(
                        session,
                        and(eq(ID, id), in(STATUS, List.of("PENDING", "PAUSED", "WAITING"))),
                        combine(
                            set(STATUS, "CANCELED"),
                            set(PICKED_BY, null),
                            set(PICKED_AT, null),
                            set(UPDATED_AT, DocumentMapper.toDate(now)),
                            set(TERMINATED_AT, DocumentMapper.toDate(now)),
                            inc(VERSION, 1)));
              });
        });
  }

  @Override
  public boolean resetRunningJob(UUID id, String nodeId) {
    return booleanMutation(
        "reset_running_job",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), eq(STATUS, "RUNNING"), eq(PICKED_BY, nodeId)),
                      combine(
                          set(STATUS, "PENDING"),
                          set(PICKED_BY, null),
                          set(PICKED_AT, null),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return intMutation(
        "reset_running_jobs",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateMany(
                      and(eq(STATUS, "RUNNING"), eq(PICKED_BY, nodeId)),
                      combine(
                          set(STATUS, "PENDING"),
                          set(PICKED_BY, null),
                          set(PICKED_AT, null),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public int cancelJobsByTag(String tag) {
    return intMutation(
        "cancel_jobs_by_tag",
        () -> {
          Instant now = Instant.now();
          // Explicit 3-status filter: ACTIVE_STATUSES includes RUNNING.
          Bson filter =
              and(
                  eq(TAGS, tag),
                  ne(JOB_TYPE, "RECURRING"),
                  in(STATUS, List.of("PENDING", "PAUSED", "WAITING")));
          try (ClientSession session = ctx.startSession()) {
            return session.withTransaction(
                () -> {
                  List<UUID> ids = new ArrayList<>();
                  for (Document doc :
                      ctx.jobs().find(session, filter).projection(new Document(ID, 1))) {
                    UUID id = doc.get(ID, UUID.class);
                    if (id != null) {
                      ids.add(id);
                    }
                  }
                  UpdateResult result =
                      ctx.jobs()
                          .updateMany(
                              session,
                              filter,
                              combine(
                                  set(STATUS, "CANCELED"),
                                  set(PICKED_BY, null),
                                  set(PICKED_AT, null),
                                  set(UPDATED_AT, DocumentMapper.toDate(now)),
                                  set(TERMINATED_AT, DocumentMapper.toDate(now)),
                                  inc(VERSION, 1)));
                  reservations.releaseByOwners(session, ids);
                  return (int) result.getModifiedCount();
                });
          }
        });
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return booleanMutation(
        "reset_failed_to_pending",
        () -> {
          return transactionalStatusMutation(
              id,
              JobStatus.PENDING,
              session ->
                  ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, "FAILED")),
                          combine(
                              set(STATUS, "PENDING"),
                              set(ATTEMPTS, 0),
                              set(LAST_ERROR, null),
                              set(SCHEDULED_TIME, DocumentMapper.toDate(Instant.now())),
                              set(PICKED_BY, null),
                              set(PICKED_AT, null),
                              unset(TERMINATED_AT),
                              set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                              inc(VERSION, 1))));
        });
  }

  @Override
  public int resetFailedToPending(JobFilter filter, int limit) {
    JobFilter failed = BulkRetryFilters.normalize(filter, limit);
    if (failed == null) {
      return 0;
    }
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            List<UUID> ids =
                query.searchLive(session, failed, limit, 0).stream().map(JobEntity::getId).toList();
            if (ids.isEmpty()) {
              return 0;
            }
            Instant now = Instant.now();
            UpdateResult result =
                ctx.jobs()
                    .updateMany(
                        session,
                        and(in(ID, ids), eq(STATUS, "FAILED")),
                        combine(
                            set(STATUS, "PENDING"),
                            set(ATTEMPTS, 0),
                            set(LAST_ERROR, null),
                            set(SCHEDULED_TIME, DocumentMapper.toDate(now)),
                            set(PICKED_BY, null),
                            set(PICKED_AT, null),
                            unset(TERMINATED_AT),
                            set(UPDATED_AT, DocumentMapper.toDate(now)),
                            inc(VERSION, 1)));
            int modified = (int) result.getModifiedCount();
            if (modified != ids.size()) {
              throw new RatchetTransientStoreException(
                  "Bulk retry selection changed concurrently: selected "
                      + ids.size()
                      + " jobs but reset "
                      + modified);
            }
            return modified;
          });
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("bulk reset failed jobs to pending", e);
    }
  }

  @Override
  public boolean transitionToPaused(UUID id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (expected == JobStatus.WAITING
        || MongoStoreContext.TERMINAL_STATUSES.contains(expected.name())) {
      return false;
    }
    return booleanMutation(
        "transition_to_paused",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), eq(STATUS, expected.name())),
                      combine(
                          set(STATUS, "PAUSED"),
                          set(PAUSED_FROM_STATUS, expected.name()),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public boolean transitionFromPaused(UUID id, JobStatus target) {
    if (!MongoStoreContext.ACTIVE_STATUSES.contains(target.name())
        || target == JobStatus.PAUSED
        || target == JobStatus.WAITING) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    return booleanMutation(
        "transition_from_paused",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), eq(STATUS, "PAUSED")),
                      combine(
                          set(STATUS, target.name()),
                          set(PAUSED_FROM_STATUS, null),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public JobStatus transitionFromPausedAtomic(UUID id) {
    return mutation(
        "transition_from_paused_atomic",
        () -> {
          Document before =
              ctx.jobs()
                  .findOneAndUpdate(
                      and(eq(ID, id), eq(STATUS, "PAUSED")),
                      // Use an update pipeline so MongoDB restores the pre-pause status from the
                      // same matched document in this atomic transition. A read followed by a
                      // separate update could race with another worker resuming or claiming the
                      // job.
                      List.of(
                          new Document(
                              "$set",
                              new Document()
                                  .append(
                                      STATUS,
                                      new Document(
                                          "$ifNull", List.of("$" + PAUSED_FROM_STATUS, "PENDING")))
                                  .append(PAUSED_FROM_STATUS, null)
                                  .append(UPDATED_AT, new Date())
                                  .append(
                                      VERSION, new Document("$add", List.of("$" + VERSION, 1))))),
                      new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
          if (before == null) {
            return null;
          }
          String pausedFrom = before.getString(PAUSED_FROM_STATUS);
          return pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
        });
  }

  private void runMutation(String operation, Runnable mutation) {
    ctx.timedStoreOperation(
        operation,
        () -> {
          mutation.run();
          return Boolean.TRUE;
        },
        ignored -> "success");
  }

  private boolean transactionalStatusMutation(
      UUID id, JobStatus status, Function<ClientSession, UpdateResult> mutation) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            UpdateResult result = mutation.apply(session);
            if (result.getModifiedCount() == 0) {
              return false;
            }
            reservations.syncForStoredJob(session, id, status);
            return true;
          });
    }
  }

  private boolean booleanMutation(String operation, BooleanSupplier mutation) {
    return ctx.timedStoreOperation(
        operation, mutation::getAsBoolean, updated -> updated ? "updated" : "miss");
  }

  private int intMutation(String operation, IntSupplier mutation) {
    return ctx.timedStoreOperation(
        operation, mutation::getAsInt, updated -> updated > 0 ? "updated" : "miss");
  }

  private <T> T mutation(String operation, Supplier<T> mutation) {
    return ctx.timedStoreOperation(
        operation, mutation, result -> result == null ? "miss" : "updated");
  }

  private static Date terminalDate(JobStatus status, Instant now) {
    return MongoStoreContext.TERMINAL_STATUSES.contains(status.name())
        ? DocumentMapper.toDate(now)
        : null;
  }
}
