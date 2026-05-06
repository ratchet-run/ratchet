package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static run.ratchet.store.mongodb.MongoFieldNames.EXPIRES_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.HEARTBEAT_TS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.LOCKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_NODE;
import static run.ratchet.store.mongodb.MongoFieldNames.STARTED_AT;

import com.mongodb.MongoCommandException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import run.ratchet.store.entity.NodeEntity;

/**
 * Distributed locks (scheduler_lock) + node heartbeats (scheduler_node) + a server-clock probe.
 *
 * <p>tryLock uses a conditional upsert: the {@code lt(EXPIRES_AT, now)} filter lets an expired lock
 * be overwritten atomically; a live one triggers the duplicate-key path. Heartbeat upsert ensures a
 * node row exists with {@code started_at} set on first write only.
 */
final class MongoNodeLockOperations {

  private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

  private final MongoStoreContext ctx;

  MongoNodeLockOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryLock(String name, Duration ttl, String nodeId) {
    Date now = DocumentMapper.toDate(Instant.now());
    Date expiresAt = DocumentMapper.toDate(Instant.now().plus(ttl));

    try {
      Document result =
          ctx.locks()
              .findOneAndUpdate(
                  and(eq(ID, name), lt(EXPIRES_AT, now)),
                  combine(
                      set(OWNER_NODE, nodeId),
                      set(LOCKED_AT, now),
                      set(EXPIRES_AT, expiresAt),
                      setOnInsert(ID, name)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
      return result != null && nodeId.equals(result.getString(OWNER_NODE));
    } catch (MongoCommandException e) {
      if (e.getErrorCode() == DUPLICATE_KEY_ERROR_CODE) {
        return false;
      }
      throw e;
    }
  }

  void unlock(String name, String nodeId) {
    ctx.locks().deleteOne(and(eq(ID, name), eq(OWNER_NODE, nodeId)));
  }

  boolean renewLock(String name, Duration extension, String nodeId) {
    Date newExpiry = DocumentMapper.toDate(Instant.now().plus(extension));
    UpdateResult result =
        ctx.locks()
            .updateOne(and(eq(ID, name), eq(OWNER_NODE, nodeId)), set(EXPIRES_AT, newExpiry));
    return result.getModifiedCount() > 0;
  }

  void upsertHeartbeat(String nodeId, Instant ts) {
    Date tsDate = DocumentMapper.toDate(ts);
    ctx.nodes()
        .updateOne(
            eq(ID, nodeId),
            combine(set(HEARTBEAT_TS, tsDate), setOnInsert(STARTED_AT, tsDate)),
            new UpdateOptions().upsert(true));
  }

  Optional<NodeEntity> findNodeById(String nodeId) {
    Document doc = ctx.nodes().find(eq(ID, nodeId)).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toNodeEntity(doc));
  }

  List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    List<NodeEntity> results = new ArrayList<>();
    for (Document doc : ctx.nodes().find(lt(HEARTBEAT_TS, DocumentMapper.toDate(cutoff)))) {
      results.add(DocumentMapper.toNodeEntity(doc));
    }
    return results;
  }

  int deleteInactiveNodesSince(Instant cutoff) {
    DeleteResult result = ctx.nodes().deleteMany(lt(HEARTBEAT_TS, DocumentMapper.toDate(cutoff)));
    return (int) result.getDeletedCount();
  }

  Instant getDatabaseTime() {
    Document result =
        ctx.database().runCommand(new Document("serverStatus", 1).append("localTime", 1));
    Date localTime = result.getDate("localTime");
    return localTime != null ? localTime.toInstant() : Instant.now();
  }
}
