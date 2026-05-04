package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.TagStore;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.bson.Document;

/**
 * Tag operations. Thin because tags are embedded directly in the job document as a BSON array
 * rather than stored in a join collection.
 */
final class MongoTagOperations implements TagStore {

  private final MongoStoreContext ctx;

  MongoTagOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void insertTags(UUID jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    ctx.jobs()
        .updateOne(
            eq(ID, jobId),
            new Document("$addToSet", new Document(TAGS, new Document("$each", tags))));
  }

  @Override
  public int deleteTagsByJobId(UUID jobId) {
    Document before =
        ctx.jobs()
            .findOneAndUpdate(
                eq(ID, jobId),
                set(TAGS, List.of()),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return 0;
    }
    List<String> oldTags = before.getList(TAGS, String.class);
    return oldTags == null ? 0 : oldTags.size();
  }

  @Override
  public List<UUID> findJobIdsByTag(String tag, int limit, int offset) {
    List<UUID> ids = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(eq(TAGS, tag))
            .projection(new Document(ID, 1))
            .sort(ascending(ID))
            .skip(offset)
            .limit(limit)) {
      ids.add(doc.get(ID, UUID.class));
    }
    return ids;
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Document doc :
        ctx.jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document(TAGS, tag)),
                    new Document(
                        "$group",
                        new Document(ID, "$" + STATUS).append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document(ID, 1))))) {
      String status = doc.getString(ID);
      if (status != null) {
        counts.put(JobStatus.valueOf(status), ((Number) doc.get("count")).longValue());
      }
    }
    return counts;
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return aggregateStringCountsByTag(
        tag, new Document("$getField", new Document("field", paramKey).append("input", "$params")));
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return aggregateStringCountsByTag(tag, "$" + PICKED_BY);
  }

  private Map<String, Long> aggregateStringCountsByTag(String tag, Object groupExpression) {
    Map<String, Long> counts = new TreeMap<>();
    for (Document doc :
        ctx.jobs()
            .aggregate(
                List.of(
                    new Document("$match", new Document(TAGS, tag)),
                    new Document(
                        "$group",
                        new Document(ID, groupExpression)
                            .append("count", new Document("$sum", 1L))),
                    new Document("$sort", new Document(ID, 1))))) {
      Object keyValue = doc.get(ID);
      if (!(keyValue instanceof String key) || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) doc.get("count")).longValue());
    }
    return counts;
  }
}
