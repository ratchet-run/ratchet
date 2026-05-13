package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_DELIVERED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_DELIVERED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_DELIVERY_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_OUTCOME;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_PAYLOAD_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_REJECTION_REASON;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_TIMEOUT;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.SignalStore;

/**
 * MongoDB implementation of {@link SignalStore}.
 *
 * <p>Signal delivery uses {@code findOneAndUpdate} (by-id) or a session-wrapped {@code updateMany}
 * (by-key) to prevent duplicate delivery when two concurrent callers target the same WAITING set.
 */
final class MongoSignalOperations implements SignalStore {

  private static final Logger log = Logger.getLogger(MongoSignalOperations.class);
  private static final int SIGNAL_DELIVERY_RESULT_WARNING_THRESHOLD = 1000;

  private final MongoStoreContext ctx;

  MongoSignalOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public List<JobEntity> findTimedOutSignalJobs(Instant now, int limit) {
    Bson filter =
        and(
            eq(STATUS, JobStatus.WAITING.name()),
            ne(SIGNAL_TIMEOUT, null),
            lte(SIGNAL_TIMEOUT, Date.from(now)));

    List<JobEntity> result = new ArrayList<>();
    for (Document doc :
        ctx.jobs().find(filter).sort(ascending(SIGNAL_TIMEOUT, "_id")).limit(Math.max(1, limit))) {
      result.add(DocumentMapper.toJobEntity(doc));
    }
    return result;
  }

  @Override
  public int deliverSignalById(
      UUID jobId,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    Bson filter = and(eq("_id", jobId), eq(STATUS, JobStatus.WAITING.name()));
    Date updatedAt = Date.from(deliveredAt != null ? deliveredAt : Instant.now());
    Bson update =
        combine(
            set(STATUS, JobStatus.PENDING.name()),
            set(SIGNAL_PAYLOAD, payload),
            set(SIGNAL_PAYLOAD_TYPE, payloadType),
            set(SIGNAL_OUTCOME, outcome),
            set(SIGNAL_REJECTION_REASON, rejectionReason),
            set(SIGNAL_DELIVERED_AT, deliveredAt != null ? Date.from(deliveredAt) : null),
            set(SIGNAL_DELIVERED_BY, deliveredBy),
            set(SIGNAL_DELIVERY_ID, deliveryId),
            set(UPDATED_AT, updatedAt));

    try {
      Document found =
          ctx.jobs()
              .findOneAndUpdate(
                  filter,
                  update,
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
      int updated = found != null ? 1 : 0;
      log.debugf("deliverSignalById(%s): %s", jobId, updated > 0 ? "delivered" : "miss");
      return updated;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("deliver signal by id", e);
    }
  }

  @Override
  public int deliverSignalByKey(
      String signalKey,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    Bson filter = and(eq(SIGNAL_KEY, signalKey), eq(STATUS, JobStatus.WAITING.name()));
    Date updatedAt = Date.from(deliveredAt != null ? deliveredAt : Instant.now());
    Bson update =
        combine(
            set(STATUS, JobStatus.PENDING.name()),
            set(SIGNAL_PAYLOAD, payload),
            set(SIGNAL_PAYLOAD_TYPE, payloadType),
            set(SIGNAL_OUTCOME, outcome),
            set(SIGNAL_REJECTION_REASON, rejectionReason),
            set(SIGNAL_DELIVERED_AT, deliveredAt != null ? Date.from(deliveredAt) : null),
            set(SIGNAL_DELIVERED_BY, deliveredBy),
            set(SIGNAL_DELIVERY_ID, deliveryId),
            set(UPDATED_AT, updatedAt));

    try (ClientSession session = ctx.startSession()) {
      UpdateResult result =
          session.withTransaction(() -> ctx.jobs().updateMany(session, filter, update));
      int updated = (int) result.getModifiedCount();
      log.debugf("deliverSignalByKey('%s'): %s job(s) unblocked", signalKey, updated);
      return updated;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("deliver signal by key", e);
    }
  }

  @Override
  public List<JobEntity> findJobsBySignalDeliveryId(String deliveryId) {
    if (deliveryId == null || deliveryId.isBlank()) {
      return List.of();
    }
    // Limit the cursor to one beyond the warning threshold so the driver never fetches an
    // unbounded result set. The sentinel (+1) document triggers the warning without OOM risk.
    List<JobEntity> result = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(eq(SIGNAL_DELIVERY_ID, deliveryId))
            .limit(SIGNAL_DELIVERY_RESULT_WARNING_THRESHOLD + 1)) {
      result.add(DocumentMapper.toJobEntity(doc));
      if (result.size() == SIGNAL_DELIVERY_RESULT_WARNING_THRESHOLD + 1) {
        log.warnf(
            "MongoDB signal delivery lookup %s returned more than %d jobs",
            deliveryId, SIGNAL_DELIVERY_RESULT_WARNING_THRESHOLD);
      }
    }
    return result;
  }
}
