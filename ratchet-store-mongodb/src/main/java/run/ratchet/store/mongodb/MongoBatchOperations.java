package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_EXECUTION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETED_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETION_PROCESSED;
import static run.ratchet.store.mongodb.MongoFieldNames.FAILED_ITEMS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OVERHEAD_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.STARTED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.SUCCESS_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_DURATION_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.TOTAL_ITEMS;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.bson.Document;

/**
 * Batch and batch-metrics collection operations. Batches drive fan-out/fan-in accounting for {@code
 * BATCH_CHILD} jobs; metrics accumulate overhead/execution splits for dashboards.
 */
final class MongoBatchOperations {

  private final MongoStoreContext ctx;

  MongoBatchOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  BatchEntity saveBatch(BatchEntity batch) {
    Document doc = DocumentMapper.toDocument(batch);
    ctx.batches().replaceOne(eq(ID, batch.getId()), doc, new ReplaceOptions().upsert(true));
    return batch;
  }

  Optional<BatchEntity> findBatchById(long batchId) {
    Document doc = ctx.batches().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchEntity(doc));
  }

  List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    List<BatchEntity> result = new ArrayList<>();
    for (Document doc : ctx.batches().find(in(ID, batchIds))) {
      result.add(DocumentMapper.toBatchEntity(doc));
    }
    return result;
  }

  BatchProgress incrementCompletedAtomic(long batchId) {
    Document doc =
        ctx.batches()
            .findOneAndUpdate(
                eq(ID, batchId),
                inc(COMPLETED_ITEMS, 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    if (doc == null) {
      throw new IllegalStateException("Batch not found: " + batchId);
    }
    return DocumentMapper.toBatchProgress(doc, batchId);
  }

  BatchProgress incrementFailedAtomic(long batchId) {
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

  boolean markBatchCompleteIfReady(long batchId) {
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

  List<Long> findRecoverableBatchIds(int limit) {
    List<Long> ids = new ArrayList<>();
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
      ids.add(doc.getLong(ID));
    }
    return ids;
  }

  boolean updateBatchTotalItems(long batchId, int totalItems) {
    UpdateResult result = ctx.batches().updateOne(eq(ID, batchId), set(TOTAL_ITEMS, totalItems));
    return result.getModifiedCount() > 0;
  }

  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    Document doc = DocumentMapper.toDocument(metrics);
    ctx.batchMetrics()
        .replaceOne(eq(ID, metrics.getBatchId()), doc, new ReplaceOptions().upsert(true));
    return metrics;
  }

  Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toBatchMetricsEntity(doc));
  }

  void addChildExecutionTime(long batchId, long durationMs) {
    ctx.batchMetrics()
        .updateOne(
            eq(ID, batchId), combine(inc(CHILD_EXECUTION_MS, durationMs), inc(SUCCESS_COUNT, 1)));
  }

  void finalizeBatchMetrics(long batchId) {
    Document doc = ctx.batchMetrics().find(eq(ID, batchId)).first();
    if (doc == null) {
      return;
    }
    Instant now = Instant.now();
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
            eq(ID, batchId),
            combine(
                set(COMPLETED_AT, DocumentMapper.toDate(now)),
                set(TOTAL_DURATION_MS, totalDurationMs),
                set(OVERHEAD_MS, overheadMs)));
  }

  void updateBatchMetricsChildCount(long batchId, int childCount) {
    ctx.batchMetrics().updateOne(eq(ID, batchId), set(CHILD_COUNT, childCount));
  }
}
