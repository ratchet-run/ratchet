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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.TagStore;

/**
 * Tag operations. Thin because tags are embedded directly in the job document as a BSON array
 * rather than stored in a join collection.
 *
 * <p>Recurring masters live in a separate collection ({@code scheduler_recurring_job}) but share
 * the same UUID space as one-shot jobs; insert/delete probe the executable collection first and
 * fall back to the recurring collection so callers don't need to know which collection owns the id.
 */
final class MongoTagOperations implements TagStore {

  private static final int MAX_STRING_COUNT_GROUPS = 1000;

  private final MongoStoreContext ctx;

  MongoTagOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void insertTags(UUID jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    try {
      Document update = new Document("$addToSet", new Document(TAGS, new Document("$each", tags)));
      UpdateResult primary = ctx.jobs().updateOne(eq(ID, jobId), update);
      if (primary.getMatchedCount() == 0) {
        ctx.recurringJobs().updateOne(eq(ID, jobId), update);
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("insert job tags", e);
    }
  }

  @Override
  public int deleteTagsByJobId(UUID jobId) {
    try {
      int removed = clearTagsIn(ctx.jobs(), jobId, -1);
      if (removed >= 0) {
        return removed;
      }
      int removedRecurring = clearTagsIn(ctx.recurringJobs(), jobId, -1);
      return Math.max(removedRecurring, 0);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete job tags", e);
    }
  }

  /**
   * Clears the tag array on a single document, returning the previous tag count, or {@code
   * notFound} when no document matched. Used by {@link #deleteTagsByJobId} to probe both
   * collections without raising on the miss case.
   */
  private int clearTagsIn(MongoCollection<Document> collection, UUID jobId, int notFound) {
    Document before =
        collection.findOneAndUpdate(
            eq(ID, jobId),
            set(TAGS, List.of()),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
    if (before == null) {
      return notFound;
    }
    List<String> oldTags = before.getList(TAGS, String.class);
    return oldTags == null ? 0 : oldTags.size();
  }

  List<UUID> findJobIdsByTag(String tag, int limit, int offset) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job ids by tag", e);
    }
  }

  Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    try {
      Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
      for (Document doc :
          ctx.jobs()
              .aggregate(
                  List.of(
                      new Document("$match", new Document(TAGS, tag)),
                      new Document(
                          "$group",
                          new Document(ID, "$" + STATUS).append("count", new Document("$sum", 1L))),
                      new Document("$limit", JobStatus.values().length),
                      new Document("$sort", new Document(ID, 1))))) {
        String status = doc.getString(ID);
        if (status != null) {
          counts.put(JobStatus.valueOf(status), ((Number) doc.get("count")).longValue());
        }
      }
      return counts;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by status for tag", e);
    }
  }

  Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    try {
      return aggregateStringCountsByTag(
          tag,
          new Document("$getField", new Document("field", paramKey).append("input", "$params")));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by param for tag", e);
    }
  }

  Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    try {
      return aggregateStringCountsByTag(tag, "$" + PICKED_BY);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by execution node for tag", e);
    }
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
                    new Document("$sort", new Document(ID, 1)),
                    new Document("$limit", MAX_STRING_COUNT_GROUPS)))) {
      Object keyValue = doc.get(ID);
      if (!(keyValue instanceof String key) || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) doc.get("count")).longValue());
    }
    return counts;
  }
}
