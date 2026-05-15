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
import static com.mongodb.client.model.Updates.unset;
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
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.bson.Document;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Job state-transition operations. Every method targets a specific {@code (id, status)}
 * precondition to stay race-free — Mongo has no row lock, so safety is structural, not
 * transactional. Cross-job side effects (e.g. bumping batch counters on success) are delegated back
 * into {@link MongoBatchOperations}.
 */
final class MongoJobLifecycleOperations
    implements JobBatchStatusStore, JobPauseStore, JobRetryStore, JobTerminalStore {

  private static final Logger log = Logger.getLogger(MongoJobLifecycleOperations.class);

  private final MongoStoreContext ctx;
  private final MongoBatchOperations batches;

  MongoJobLifecycleOperations(MongoStoreContext ctx, MongoBatchOperations batches) {
    this.ctx = ctx;
    this.batches = batches;
  }

  @Override
  public void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    runMutation(
        "update job status",
        () -> {
          Instant now = Instant.now();
          ctx.jobs()
              .updateOne(
                  eq(ID, id),
                  combine(
                      set(STATUS, status.name()),
                      set(LAST_ERROR, errorMessage),
                      set(UPDATED_AT, DocumentMapper.toDate(now)),
                      set(TERMINATED_AT, terminalDate(status, now)),
                      inc(VERSION, 1)));
        });
  }

  @Override
  public boolean compareAndSwapStatus(
      UUID id, JobStatus expected, JobStatus newStatus, String error) {
    try {
      Instant now = Instant.now();
      if ((newStatus == JobStatus.FAILED || newStatus == JobStatus.CANCELED)
          && expected == JobStatus.RUNNING) {
        UpdateResult result =
            ctx.jobs()
                .updateOne(
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
        return result.getModifiedCount() > 0;
      }
      UpdateResult result =
          ctx.jobs()
              .updateOne(
                  and(eq(ID, id), eq(STATUS, expected.name())),
                  combine(
                      set(STATUS, newStatus.name()),
                      set(LAST_ERROR, error),
                      set(UPDATED_AT, DocumentMapper.toDate(now)),
                      set(TERMINATED_AT, terminalDate(newStatus, now)),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("compare-and-swap status", e);
    }
  }

  @Override
  public int incrementRetryAttempt(UUID id) {
    return intMutation(
        "increment retry attempt",
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
        "pick up job",
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
    try {
      Instant now = Instant.now();
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
                      set(TERMINATED_AT, DocumentMapper.toDate(now)),
                      set(EXECUTION_DURATION_MS, durationMs),
                      set(QUEUE_WAIT_MS, queueWaitMs),
                      set(LAST_ERROR, null),
                      set(UPDATED_AT, DocumentMapper.toDate(now)),
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded", e);
    }
  }

  @Override
  public boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      Instant now = Instant.now();
      UpdateResult result =
          ctx.jobs()
              .updateOne(
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
                      inc(VERSION, 1)));
      return result.getModifiedCount() > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded minimally", e);
    }
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
            batches.incrementCompletedAtomic(session, batchId);
            return true;
          });
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded and update batch", e);
    }
  }

  @Override
  public boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    return booleanMutation(
        "schedule job retry",
        () -> {
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), in(STATUS, List.of("RUNNING", "WAITING", "FAILED"))),
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
  public boolean pauseRecurring(UUID id) {
    return booleanMutation(
        "pause recurring job",
        () -> {
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
        });
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    return booleanMutation(
        "resume recurring job",
        () -> {
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
        });
  }

  @Override
  public boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return booleanMutation(
        "mark job failed terminal",
        () -> {
          Instant now = Instant.now();
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
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
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public boolean cancelJob(UUID id) {
    return booleanMutation(
        "cancel job",
        () -> {
          Instant now = Instant.now();
          UpdateResult runningResult =
              ctx.jobs()
                  .updateOne(
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
            return true;
          }
          UpdateResult result =
              ctx.jobs()
                  .updateOne(
                      and(eq(ID, id), in(STATUS, List.of("PENDING", "PAUSED", "WAITING"))),
                      combine(
                          set(STATUS, "CANCELED"),
                          set(PICKED_BY, null),
                          set(PICKED_AT, null),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public boolean resetRunningJob(UUID id, String nodeId) {
    return booleanMutation(
        "reset running job",
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
        "reset running jobs",
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
        "cancel jobs by tag",
        () -> {
          Instant now = Instant.now();
          // Explicit 3-status filter: ACTIVE_STATUSES includes RUNNING.
          UpdateResult result =
              ctx.jobs()
                  .updateMany(
                      and(
                          eq(TAGS, tag),
                          ne(JOB_TYPE, "RECURRING"),
                          in(STATUS, List.of("PENDING", "PAUSED", "WAITING"))),
                      combine(
                          set(STATUS, "CANCELED"),
                          set(PICKED_BY, null),
                          set(PICKED_AT, null),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return intMutation(
        "cancel recurring jobs by tag",
        () -> {
          Instant now = Instant.now();
          UpdateResult result =
              ctx.jobs()
                  .updateMany(
                      and(
                          eq(TAGS, tag),
                          eq(JOB_TYPE, "RECURRING"),
                          in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                      combine(
                          set(STATUS, "CANCELED"),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return intMutation(
        "cancel recurring job by business key",
        () -> {
          Instant now = Instant.now();
          UpdateResult result =
              ctx.jobs()
                  .updateMany(
                      and(
                          eq(BUSINESS_KEY, businessKey),
                          eq(JOB_TYPE, "RECURRING"),
                          in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                      combine(
                          set(STATUS, "CANCELED"),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
    if (businessKeys.isEmpty()) {
      return 0;
    }
    return intMutation(
        "cancel recurring jobs by business keys",
        () -> {
          Instant now = Instant.now();
          UpdateResult result =
              ctx.jobs()
                  .updateMany(
                      and(
                          in(BUSINESS_KEY, businessKeys),
                          eq(JOB_TYPE, "RECURRING"),
                          in(STATUS, MongoStoreContext.ACTIVE_STATUSES)),
                      combine(
                          set(STATUS, "CANCELED"),
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    return intMutation(
        "cancel orphaned recurring annotation jobs",
        () -> {
          Instant now = Instant.now();
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
                          set(UPDATED_AT, DocumentMapper.toDate(now)),
                          set(TERMINATED_AT, DocumentMapper.toDate(now)),
                          inc(VERSION, 1)));
          return (int) result.getModifiedCount();
        });
  }

  @Override
  public boolean resetFailedToPending(UUID id) {
    return booleanMutation(
        "reset failed job to pending",
        () -> {
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
                          unset(TERMINATED_AT),
                          set(UPDATED_AT, DocumentMapper.toDate(Instant.now())),
                          inc(VERSION, 1)));
          return result.getModifiedCount() > 0;
        });
  }

  @Override
  public boolean transitionToPaused(UUID id, JobStatus expected) {
    return booleanMutation(
        "transition job to paused",
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
    return booleanMutation(
        "transition job from paused",
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
        "transition job from paused atomically",
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
    try {
      mutation.run();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException(operation, e);
    }
  }

  private boolean booleanMutation(String operation, BooleanSupplier mutation) {
    try {
      return mutation.getAsBoolean();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException(operation, e);
    }
  }

  private int intMutation(String operation, IntSupplier mutation) {
    try {
      return mutation.getAsInt();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException(operation, e);
    }
  }

  private <T> T mutation(String operation, Supplier<T> mutation) {
    try {
      return mutation.get();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException(operation, e);
    }
  }

  private static Date terminalDate(JobStatus status, Instant now) {
    return MongoStoreContext.TERMINAL_STATUSES.contains(status.name())
        ? DocumentMapper.toDate(now)
        : null;
  }
}
