package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_DELIVERED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_DELIVERED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.SIGNAL_TIMEOUT;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.SignalStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * MongoDB implementation of {@link SignalStore}.
 *
 * <p>Signal delivery uses {@code findOneAndUpdate} (by-id) or a session-wrapped {@code updateMany}
 * (by-key) to prevent duplicate delivery when two concurrent callers target the same WAITING set.
 */
final class MongoSignalOperations implements SignalStore {

  private static final Logger log = Logger.getLogger(MongoSignalOperations.class);

  private final MongoStoreContext ctx;

  MongoSignalOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public List<JobEntity> findTimedOutSignalJobs(Instant now) {
    Bson filter =
        and(
            eq(STATUS, JobStatus.WAITING.name()),
            ne(SIGNAL_TIMEOUT, null),
            lte(SIGNAL_TIMEOUT, Date.from(now)));

    List<JobEntity> result = new ArrayList<>();
    for (Document doc : ctx.jobs().find(filter)) {
      result.add(DocumentMapper.toJobEntity(doc));
    }
    return result;
  }

  @Override
  public int deliverSignalById(
      UUID jobId, String payload, String deliveredBy, Instant deliveredAt) {
    Bson filter = and(eq("_id", jobId), eq(STATUS, JobStatus.WAITING.name()));
    Bson update =
        combine(
            set(STATUS, JobStatus.PENDING.name()),
            set(SIGNAL_PAYLOAD, payload),
            set(SIGNAL_DELIVERED_AT, deliveredAt != null ? Date.from(deliveredAt) : null),
            set(SIGNAL_DELIVERED_BY, deliveredBy),
            set(UPDATED_AT, Date.from(Instant.now())));

    Document found =
        ctx.jobs()
            .findOneAndUpdate(
                filter,
                update,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    int updated = found != null ? 1 : 0;
    log.debugf("deliverSignalById(%s): %s", jobId, updated > 0 ? "delivered" : "miss");
    return updated;
  }

  @Override
  public int deliverSignalByKey(
      String signalKey, String payload, String deliveredBy, Instant deliveredAt) {
    Bson filter = and(eq(SIGNAL_KEY, signalKey), eq(STATUS, JobStatus.WAITING.name()));
    Bson update =
        combine(
            set(STATUS, JobStatus.PENDING.name()),
            set(SIGNAL_PAYLOAD, payload),
            set(SIGNAL_DELIVERED_AT, deliveredAt != null ? Date.from(deliveredAt) : null),
            set(SIGNAL_DELIVERED_BY, deliveredBy),
            set(UPDATED_AT, Date.from(Instant.now())));

    UpdateResult result;
    try (ClientSession session = ctx.startSession()) {
      result = session.withTransaction(() -> ctx.jobs().updateMany(session, filter, update));
    }
    int updated = (int) result.getModifiedCount();
    log.debugf("deliverSignalByKey('%s'): %s job(s) unblocked", signalKey, updated);
    return updated;
  }
}
