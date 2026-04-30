package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPTS;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
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
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;

/**
 * Job state-transition operations. Every method targets a specific {@code (id, status)}
 * precondition to stay race-free — Mongo has no row lock, so safety is structural, not
 * transactional. Cross-job side effects (e.g. bumping batch counters on success) are delegated back
 * into {@link MongoBatchOperations}.
 */
final class MongoJobLifecycleOperations {

  private final MongoStoreContext ctx;
  private final MongoBatchOperations batches;

  MongoJobLifecycleOperations(MongoStoreContext ctx, MongoBatchOperations batches) {
    this.ctx = ctx;
    this.batches = batches;
  }

  void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    ctx.jobs()
        .updateOne(
            eq(ID, id),
            combine(
                set(STATUS, status.name()),
                set(LAST_ERROR, errorMessage),
                set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                inc(VERSION, 1)));
  }

  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, expected.name())),
                  combine(
                      set(STATUS, newStatus.name()),
                      set(LAST_ERROR, error),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("compare-and-swap status", e);
    }
  }

  int incrementRetryAttempt(UUID id) {
    Document doc =
        ctx.jobs()
            .findOneAndUpdate(
                and(eq(ID, id), eq(STATUS, "RUNNING")),
                combine(
                    inc(ATTEMPTS, 1),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      return -1;
    }
    return doc.getInteger(ATTEMPTS);
  }

  boolean tryPickUpJob(UUID id, String nodeId) {
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
  }

  boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, "RUNNING")),
                  combine(
                      set(STATUS, "SUCCEEDED"),
                      set(JOB_RESULT, resultJson),
                      set(RESULT_TYPE, resultType),
                      set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                      set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                      set(EXECUTION_DURATION_MS, durationMs),
                      set(QUEUE_WAIT_MS, queueWaitMs),
                      set(LAST_ERROR, null),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded", e);
    }
  }

  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, "RUNNING")),
                  combine(
                      set(STATUS, "SUCCEEDED"),
                      set(EXECUTION_START_TIME, DocumentMapper.toDate(start)),
                      set(EXECUTION_END_TIME, DocumentMapper.toDate(end)),
                      set(EXECUTION_DURATION_MS, durationMs),
                      set(QUEUE_WAIT_MS, queueWaitMs),
                      set(LAST_ERROR, null),
                      set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded minimally", e);
    }
  }

  boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            try {
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
                              set(EXECUTION_DURATION_MS, durationMs),
                              set(QUEUE_WAIT_MS, queueWaitMs),
                              set(LAST_ERROR, null),
                              set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                              inc(VERSION, 1)));
              if (result.getModifiedCount() == 0) {
                return false;
              }
              batches.incrementCompletedAtomic(session, batchId);
              return true;
            } catch (RuntimeException e) {
              throw ctx.translateTransientStoreException("mark job succeeded and update batch", e);
            }
          });
    }
  }

  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), in(STATUS, List.of("RUNNING", "FAILED"))),
                combine(
                    set(STATUS, "PENDING"),
                    set(SCHEDULED_TIME, DocumentMapper.toDate(newScheduledTime)),
                    set(ATTEMPTS, attempts),
                    set(LAST_ERROR, error),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean pauseRecurring(UUID id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(JOB_TYPE, "RECURRING"), eq(STATUS, "PENDING")),
                combine(
                    set(STATUS, "PAUSED"),
                    set(PAUSED_FROM_STATUS, "PENDING"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean resumeRecurring(UUID id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(JOB_TYPE, "RECURRING"), eq(STATUS, "PAUSED")),
                combine(
                    set(STATUS, "PENDING"),
                    set(PAUSED_FROM_STATUS, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "RUNNING")),
                combine(
                    set(STATUS, "FAILED"),
                    set(LAST_ERROR, terminalError),
                    set(ATTEMPTS, totalAttempts),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean cancelJob(UUID id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), in(STATUS, List.of("PENDING", "RUNNING", "PAUSED"))),
                combine(
                    set(STATUS, "CANCELED"),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean resetRunningJob(UUID id, String nodeId) {
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
  }

  int resetRunningJobs(String nodeId) {
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
  }

  int cancelRecurringJobsByTag(String tag) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(TAGS, tag),
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  int cancelRecurringJobByBusinessKey(String businessKey) {
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(BUSINESS_KEY, businessKey),
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    UpdateResult result =
        ctx.jobs()
            .updateMany(
                and(
                    eq(JOB_TYPE, "RECURRING"),
                    in(STATUS, MongoStoreContext.ACTIVE_STATUSES),
                    lt(CREATED_AT, DocumentMapper.toDate(nodeStartTime)),
                    ne(BUSINESS_KEY, null),
                    nin(BUSINESS_KEY, registeredIds)),
                combine(
                    set(STATUS, "CANCELED"),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return (int) result.getModifiedCount();
  }

  boolean resetFailedToPending(UUID id) {
    UpdateResult result =
        ctx.jobs()
            .updateOne(
                and(eq(ID, id), eq(STATUS, "FAILED")),
                combine(
                    set(STATUS, "PENDING"),
                    set(ATTEMPTS, 0),
                    set(LAST_ERROR, null),
                    set(SCHEDULED_TIME, DocumentMapper.toDate(Instant.now())),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                    inc(VERSION, 1)));
    return result.getModifiedCount() > 0;
  }

  boolean transitionToPaused(UUID id, JobStatus expected) {
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
  }

  boolean transitionFromPaused(UUID id, JobStatus target) {
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
  }

  JobStatus transitionFromPausedAtomic(UUID id) {
    Document before =
        ctx.jobs()
            .findOneAndUpdate(
                and(eq(ID, id), eq(STATUS, "PAUSED")),
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
                            .append(VERSION, new Document("$add", List.of("$" + VERSION, 1))))),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return null;
    }
    String pausedFrom = before.getString(PAUSED_FROM_STATUS);
    return pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
  }
}
