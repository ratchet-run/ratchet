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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import run.ratchet.api.JobPriority;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * Claim pipeline: candidate planning via {@code $match → $project → $sort → $limit} with index
 * hints, followed by a single atomic {@code bulkWrite(ordered=false)} that claims all candidates
 * server-side, plus a single {@code find} read-back. Total round-trips per claim cycle: 2.
 */
final class MongoJobClaimOperations {

  private static final Logger log = Logger.getLogger(MongoJobClaimOperations.class);

  private final MongoStoreContext ctx;

  MongoJobClaimOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
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

  /**
   * Claims a set of candidate jobs in two round-trips: a single {@code bulkWrite(ordered=false)}
   * for the conditional updates, then one {@code find} to read the claimed documents back.
   *
   * <p><b>Invariant required of callers:</b> no two concurrent invocations on the same node may
   * share candidate IDs. The read-back filter is {@code _id $in ids AND picked_by = node AND status
   * = RUNNING}; if two threads on the same node have overlapping {@code ids}, the read-back cannot
   * tell whose claim won and may double-return a document. Current callers ({@link
   * run.ratchet.ri.core.Poller} sequentially per disjoint job type, and the recurring
   * executor on a disjoint type) honor this invariant.
   *
   * <p>Documents transitioned away by another process (e.g., orphan recovery flipping
   * RUNNING→PENDING) between the bulk write and the read-back are dropped from the result — orphan
   * recovery has already taken responsibility for them, so dropping is safer than potentially
   * double-executing.
   */
  private <T> List<T> claimByIds(List<Long> ids, String nodeId, Function<Document, T> mapper) {
    if (ids.isEmpty()) {
      return List.of();
    }

    Date nowDate = DocumentMapper.toDate(Instant.now());
    List<UpdateOneModel<Document>> ops = new ArrayList<>(ids.size());
    for (Long id : ids) {
      ops.add(
          new UpdateOneModel<>(
              and(eq(ID, id), eq(STATUS, "PENDING")),
              combine(
                  set(STATUS, "RUNNING"),
                  set(PICKED_BY, nodeId),
                  set(PICKED_AT, nowDate),
                  set(UPDATED_AT, nowDate),
                  inc(VERSION, 1))));
    }

    long matched;
    try {
      BulkWriteResult result = ctx.jobs().bulkWrite(ops, new BulkWriteOptions().ordered(false));
      matched = result.getMatchedCount();
    } catch (MongoBulkWriteException e) {
      // ordered=false: server applied every op; partial successes are durable. Log per-op
      // failures and continue with the read-back of whatever did succeed.
      matched = e.getWriteResult().getMatchedCount();
      for (BulkWriteError err : e.getWriteErrors()) {
        log.warnf("Bulk claim error at index %d: %s", err.getIndex(), err.getMessage());
      }
    }

    if (matched == 0) {
      return List.of();
    }

    Map<Long, T> byId = new HashMap<>(ids.size());
    for (Document doc :
        ctx.jobs()
            .find(
                and(
                    new Document(ID, new Document("$in", ids)),
                    eq(PICKED_BY, nodeId),
                    eq(STATUS, "RUNNING")))) {
      byId.put(doc.getLong(ID), mapper.apply(doc));
    }
    List<T> ordered = new ArrayList<>(byId.size());
    for (Long id : ids) {
      T claim = byId.get(id);
      if (claim != null) {
        ordered.add(claim);
      }
    }
    return ordered;
  }
}
