package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.NEXT_FIRE;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import run.ratchet.api.JobPriority;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * Claim pipeline: candidate planning via {@code $match → $project → $sort → $limit} with index
 * hints, followed by per-candidate atomic {@code findOneAndUpdate} executed in parallel.
 *
 * <p>The caller-owned {@link ExecutorService} is intentionally not managed here: production
 * deployments pass the Jakarta-managed executor supplied by the configured executor provider.
 * Operation classes are constructed via {@code new} so CDI does not call lifecycle hooks on them.
 */
final class MongoJobClaimOperations {

  private static final Logger log = Logger.getLogger(MongoJobClaimOperations.class);

  private final MongoStoreContext ctx;
  private final ExecutorService claimExecutor;

  MongoJobClaimOperations(MongoStoreContext ctx, ExecutorService claimExecutor) {
    this.ctx = ctx;
    this.claimExecutor = claimExecutor;
  }

  List<JobEntity> claimNextBatch(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(
            MongoStoreContext.EXECUTABLE_JOB_TYPES, SCHEDULED_TIME, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  List<JobClaimDto> claimNextBatchOptimized(JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !MongoStoreContext.isPollerExecutable(jobType)) {
      return List.of();
    }
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of(jobType.name()), SCHEDULED_TIME, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobClaimDto);
  }

  List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    List<Long> candidateIds =
        findCandidatesByBoostedPriority(List.of("RECURRING"), NEXT_FIRE, limit);
    return claimByIds(candidateIds, nodeId, DocumentMapper::toJobEntity);
  }

  /** Finds candidate job IDs sorted by effective priority (raw priority + age-based boost). */
  private List<Long> findCandidatesByBoostedPriority(
      List<String> jobTypes, String timeColumn, int limit) {
    if (limit <= 0 || jobTypes.isEmpty()) {
      return List.of();
    }

    Date now = DocumentMapper.toDate(Instant.now());
    Bson match =
        new Document(
            "$match",
            new Document(STATUS, "PENDING")
                .append(JOB_TYPE, new Document("$in", jobTypes))
                .append(timeColumn, new Document("$lte", now)));
    Bson project =
        new Document(
            "$project",
            new Document(ID, 1)
                .append(timeColumn, 1)
                .append("effective_priority", effectivePriorityExpression(timeColumn, now)));
    Bson sort =
        new Document(
            "$sort", new Document("effective_priority", -1).append(timeColumn, 1).append(ID, 1));
    Bson batchLimit = new Document("$limit", limit);

    var query = ctx.jobs().aggregate(List.of(match, project, sort, batchLimit)).allowDiskUse(true);

    if (NEXT_FIRE.equals(timeColumn)) {
      query.hintString(MongoIndexHints.JOB_CLAIM_RECURRING);
    } else {
      query.hintString(MongoIndexHints.JOB_CLAIM_EXEC);
    }

    List<Long> ids = new ArrayList<>(limit);
    for (Document doc : query) {
      ids.add(doc.getLong(ID));
    }
    return ids;
  }

  private Object effectivePriorityExpression(String timeColumn, Date now) {
    Object priorityExpression =
        new Document("$ifNull", List.of("$" + PRIORITY, JobPriority.NORMAL.ordinal()));
    int priorityBoostInterval = ctx.priorityBoostIntervalMinutes();
    if (priorityBoostInterval <= 0) {
      return priorityExpression;
    }
    Object ageMillisExpression =
        new Document(
            "$max", List.of(0L, new Document("$subtract", List.of(now, "$" + timeColumn))));
    Object ageMinutesExpression =
        new Document("$floor", new Document("$divide", List.of(ageMillisExpression, 60_000L)));
    Object boostExpression =
        new Document(
            "$floor",
            new Document("$divide", List.of(ageMinutesExpression, priorityBoostInterval)));
    return new Document("$add", List.of(priorityExpression, boostExpression));
  }

  private <T> List<T> claimByIds(List<Long> ids, String nodeId, Function<Document, T> mapper) {
    Date nowDate = DocumentMapper.toDate(Instant.now());
    FindOneAndUpdateOptions opts =
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);

    List<CompletableFuture<Document>> futures =
        ids.stream()
            .map(
                id ->
                    CompletableFuture.supplyAsync(
                        () ->
                            ctx.jobs()
                                .findOneAndUpdate(
                                    and(eq(ID, id), eq(STATUS, "PENDING")),
                                    combine(
                                        set(STATUS, "RUNNING"),
                                        set(PICKED_BY, nodeId),
                                        set(PICKED_AT, nowDate),
                                        set(UPDATED_AT, nowDate),
                                        inc(VERSION, 1)),
                                    opts),
                        claimExecutor))
            .toList();

    List<T> claimed = new ArrayList<>();
    for (var future : futures) {
      try {
        Document doc = future.join();
        if (doc != null) {
          claimed.add(mapper.apply(doc));
        }
      } catch (CompletionException e) {
        log.warnf("Claim error: %s", e.getCause().getMessage());
      }
    }
    return claimed;
  }
}
