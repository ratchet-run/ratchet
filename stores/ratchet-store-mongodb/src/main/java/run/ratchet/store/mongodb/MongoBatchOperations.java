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
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_EXECUTION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETED_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETION_PROCESSED;
import static run.ratchet.store.mongodb.MongoFieldNames.DEPENDS_ON;
import static run.ratchet.store.mongodb.MongoFieldNames.FAILED_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.OVERHEAD_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PAUSED_FROM_STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.STARTED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.SUCCESS_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.ChildResult;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.spi.BatchStore;

/**
 * Batch and batch-metrics collection operations. Batches drive fan-out/fan-in accounting for {@code
 * BATCH_CHILD} jobs; metrics accumulate overhead/execution splits for dashboards.
 */
final class MongoBatchOperations implements BatchStore {

  private final MongoStoreContext ctx;
  private final Clock clock;

  MongoBatchOperations(MongoStoreContext ctx) {
    this(ctx, Clock.systemUTC());
  }

  MongoBatchOperations(MongoStoreContext ctx, Clock clock) {
    this.ctx = ctx;
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public int cancelBatchChildren(UUID batchId) {
    return changeBatchChildren(batchId, "cancel");
  }

  @Override
  public int pauseBatchChildren(UUID batchId) {
    return changeBatchChildren(batchId, "pause");
  }

  @Override
  public int resumeBatchChildren(UUID batchId) {
    return changeBatchChildren(batchId, "resume");
  }

  private int changeBatchChildren(UUID batchId, String operation) {
    List<String> statuses =
        switch (operation) {
          case "cancel" -> List.of("PENDING", "PAUSED", "WAITING");
          case "pause" -> List.of("PENDING");
          default -> List.of("PAUSED");
        };
    int total = 0;
    while (true) {
      int changed;
      try (ClientSession session = ctx.startSession()) {
        changed =
            session.withTransaction(
                () -> {
                  if (!operation.equals("cancel")) {
                    String expected = operation.equals("pause") ? "PAUSED" : "PENDING";
                    // Fence every chunk against an opposite parent transition or repair sweep.
                    if (ctx.jobs()
                            .updateOne(
                                session,
                                and(eq(ID, batchId), eq(STATUS, expected)),
                                inc(VERSION, 1))
                            .getMatchedCount()
                        == 0) {
                      return 0;
                    }
                  }
                  List<UUID> ids = new ArrayList<>();
                  for (Document child :
                      ctx.jobs()
                          .find(
                              session,
                              and(
                                  eq(DEPENDS_ON, batchId),
                                  eq(JOB_TYPE, "BATCH_CHILD"),
                                  in(STATUS, statuses)))
                          .sort(ascending(ID))
                          .limit(1000)) {
                    ids.add(child.get(ID, UUID.class));
                  }
                  if (ids.isEmpty()) {
                    return 0;
                  }
                  var filter = and(in(ID, ids), in(STATUS, statuses));
                  Date now = Date.from(clock.instant());
                  long count;
                  if (operation.equals("resume")) {
                    count =
                        ctx.jobs()
                            .updateMany(
                                session,
                                filter,
                                List.of(
                                    new Document(
                                        "$set",
                                        new Document(
                                                STATUS,
                                                new Document(
                                                    "$ifNull",
                                                    List.of("$" + PAUSED_FROM_STATUS, "PENDING")))
                                            .append(PAUSED_FROM_STATUS, null)
                                            .append(UPDATED_AT, now)
                                            .append(
                                                VERSION,
                                                new Document("$add", List.of("$" + VERSION, 1))))))
                            .getModifiedCount();
                  } else if (operation.equals("pause")) {
                    count =
                        ctx.jobs()
                            .updateMany(
                                session,
                                filter,
                                combine(
                                    set(STATUS, "PAUSED"),
                                    set(PAUSED_FROM_STATUS, "PENDING"),
                                    set(UPDATED_AT, now),
                                    inc(VERSION, 1)))
                            .getModifiedCount();
                  } else {
                    count =
                        ctx.jobs()
                            .updateMany(
                                session,
                                filter,
                                combine(
                                    set(STATUS, "CANCELED"),
                                    set(PICKED_BY, null),
                                    set(PICKED_AT, null),
                                    set(UPDATED_AT, now),
                                    set(TERMINATED_AT, now),
                                    inc(VERSION, 1)))
                            .getModifiedCount();
                    ctx.businessKeyReservations().deleteMany(session, in(OWNER_JOB_ID, ids));
                    if (count > 0) {
                      // An int keeps failed_items an Int32, as the per-child increments store it.
                      ctx.batches()
                          .updateOne(
                              session, eq(ID, batchId), inc(FAILED_ITEMS, Math.toIntExact(count)));
                    }
                  }
                  return Math.toIntExact(count);
                });
      }
      total += changed;
      if (changed == 0) {
        return total;
      }
    }
  }

  @Override
  public void closeBatch(UUID batchId) {
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> ctx.batches().updateOne(session, eq(ID, batchId), set(COMPLETION_PROCESSED, true)));
    }
  }

  @Override
  public List<UUID> findCancelledOpenBatchIds(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    List<UUID> ids = new ArrayList<>();
    List<Document> pipeline =
        List.of(
            new Document("$match", new Document(COMPLETION_PROCESSED, false)),
            new Document(
                "$lookup",
                new Document("from", ctx.jobs().getNamespace().getCollectionName())
                    .append("localField", ID)
                    .append("foreignField", ID)
                    .append("as", "parent")),
            new Document("$match", new Document("parent." + STATUS, "CANCELED")),
            new Document("$limit", limit));
    for (Document batch : ctx.batches().aggregate(pipeline)) {
      ids.add(batch.get(ID, UUID.class));
    }
    return ids;
  }

  @Override
  public List<ChildResult> findSucceededChildResults(UUID batchId, UUID afterJobId, int limit) {
    if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
    Bson filter =
        and(eq(DEPENDS_ON, batchId), eq(JOB_TYPE, "BATCH_CHILD"), eq(STATUS, "SUCCEEDED"));
    if (afterJobId != null) filter = and(filter, gt(ID, afterJobId));
    List<ChildResult> results = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .withReadPreference(ReadPreference.primary())
            .withReadConcern(ReadConcern.MAJORITY)
            .find(filter)
            .sort(ascending(ID))
            .limit(limit)) {
      results.add(
          new ChildResult(
              doc.get(ID, UUID.class), doc.getString("job_result"), doc.getString("result_type")));
    }
    return results;
  }

  @Override
  public boolean claimFold(UUID batchId, Instant now, Instant staleBefore) {
    return ctx.batches()
            .updateOne(
                and(
                    eq(ID, batchId),
                    eq(COMPLETION_PROCESSED, false),
                    or(eq("fold_claimed_at", null), lt("fold_claimed_at", Date.from(staleBefore)))),
                set("fold_claimed_at", Date.from(now)))
            .getModifiedCount()
        == 1;
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    Document doc = DocumentMapper.toDocument(batch);
    ctx.batches().replaceOne(eq(ID, batch.getId()), doc, new ReplaceOptions().upsert(true));
    return batch;
  }

  BatchEntity saveBatch(ClientSession session, BatchEntity batch) {
    ctx.batches()
        .replaceOne(
            session,
            eq(ID, batch.getId()),
            DocumentMapper.toDocument(batch),
            new ReplaceOptions().upsert(true));
    return batch;
  }

  @Override
  public Optional<BatchEntity> findBatchById(UUID batchId) {
    Document doc = ctx.batches().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchEntity(doc));
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<UUID> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    List<BatchEntity> result = new ArrayList<>();
    for (Document doc : ctx.batches().find(in(ID, batchIds))) {
      result.add(DocumentMapper.toBatchEntity(doc));
    }
    return result;
  }

  @Override
  public BatchProgress incrementCompletedAtomic(UUID batchId) {
    return incrementCompletedAtomic(null, batchId);
  }

  BatchProgress incrementCompletedAtomic(ClientSession session, UUID batchId) {
    FindOneAndUpdateOptions options =
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    Document doc =
        session == null
            ? ctx.batches().findOneAndUpdate(eq(ID, batchId), inc(COMPLETED_ITEMS, 1), options)
            : ctx.batches()
                .findOneAndUpdate(session, eq(ID, batchId), inc(COMPLETED_ITEMS, 1), options);
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public BatchProgress incrementFailedAtomic(UUID batchId) {
    Document doc =
        ctx.batches()
            .findOneAndUpdate(
                eq(ID, batchId),
                inc(FAILED_ITEMS, 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  @Override
  public boolean markBatchCompleteIfReady(UUID batchId) {
    UpdateResult result =
        ctx.batches()
            .updateOne(
                and(
                    eq(ID, batchId),
                    eq(COMPLETION_PROCESSED, false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document(
                                    "$add", List.of("$" + COMPLETED_ITEMS, "$" + FAILED_ITEMS)),
                                "$" + TOTAL_ITEMS)))),
                set(COMPLETION_PROCESSED, true));
    return result.getModifiedCount() > 0;
  }

  @Override
  public List<UUID> findRecoverableBatchIds(int limit) {
    List<UUID> ids = new ArrayList<>();
    FindIterable<Document> results =
        ctx.batches()
            .find(
                and(
                    eq(COMPLETION_PROCESSED, false),
                    new Document(
                        "$expr",
                        new Document(
                            "$gte",
                            List.of(
                                new Document(
                                    "$add", List.of("$" + COMPLETED_ITEMS, "$" + FAILED_ITEMS)),
                                "$" + TOTAL_ITEMS)))))
            .projection(new Document(ID, 1))
            .limit(limit);
    for (Document doc : results) {
      ids.add(doc.get(ID, UUID.class));
    }
    return ids;
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    UpdateResult result = ctx.batches().updateOne(eq(ID, batchId), set(TOTAL_ITEMS, totalItems));
    return result.getModifiedCount() > 0;
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    Document doc = DocumentMapper.toDocument(metrics);
    ctx.batchMetrics()
        .replaceOne(eq(ID, metrics.getBatchId()), doc, new ReplaceOptions().upsert(true));
    return metrics;
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchMetricsEntity(doc));
  }

  @Override
  public void addChildExecutionTime(UUID batchId, long durationMs) {
    ctx.batchMetrics()
        .updateOne(
            eq(ID, batchId), combine(inc(CHILD_EXECUTION_MS, durationMs), inc(SUCCESS_COUNT, 1)));
  }

  @Override
  public void finalizeBatchMetrics(UUID batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    if (doc == null) {
      return;
    }
    Instant now = Instant.now(clock);
    Date startedAt = doc.getDate(STARTED_AT);
    Long childExecutionMs = doc.getLong(CHILD_EXECUTION_MS);

    Long totalDurationMs = null;
    Long overheadMs = null;
    if (startedAt != null) {
      totalDurationMs = Duration.between(startedAt.toInstant(), now).toMillis();
      if (childExecutionMs != null) {
        overheadMs = totalDurationMs - childExecutionMs;
      }
    }

    ctx.batchMetrics()
        .updateOne(
            and(eq(ID, batchId), or(exists(COMPLETED_AT, false), eq(COMPLETED_AT, null))),
            combine(
                set(COMPLETED_AT, DocumentMapper.toDate(now)),
                set(TOTAL_DURATION_MS, totalDurationMs),
                set(OVERHEAD_MS, overheadMs)));
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    ctx.batchMetrics().updateOne(eq(ID, batchId), set(CHILD_COUNT, childCount));
  }
}
