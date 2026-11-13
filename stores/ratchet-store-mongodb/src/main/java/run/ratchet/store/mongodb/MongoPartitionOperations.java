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
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoStoreContext.TRANSACTION_OPTIONS;

import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.spi.PartitionStore;

/** Partition generation writes share one session transaction with job and batch creation. */
final class MongoPartitionOperations implements PartitionStore {
  private final MongoStoreContext ctx;
  private final MongoJobCrudOperations jobs;
  private final MongoBatchOperations batches;

  MongoPartitionOperations(
      MongoStoreContext ctx, MongoJobCrudOperations jobs, MongoBatchOperations batches) {
    this.ctx = ctx;
    this.jobs = jobs;
    this.batches = batches;
  }

  // Read the latest committed generation; a lagging secondary can return the previous generation.
  private MongoCollection<Document> sets() {
    return ctx.database()
        .getCollection("scheduler_partition_set")
        .withReadPreference(ReadPreference.primary());
  }

  private MongoCollection<Document> rows() {
    return ctx.database()
        .getCollection("scheduler_partition")
        .withReadPreference(ReadPreference.primary());
  }

  @Override
  public Optional<PartitionSetEntity> findPartitionSet(String setKey) {
    return Optional.ofNullable(sets().find(eq("set_key", setKey)).first()).map(this::mapSet);
  }

  @Override
  public Optional<PartitionSetEntity> findPartitionSetById(UUID setId) {
    return Optional.ofNullable(sets().find(eq("_id", setId)).first()).map(this::mapSet);
  }

  @Override
  public boolean createOrAdvance(PartitionGenerationWrite write) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            PartitionSetEntity set = write.set();
            if (write.expectedVersion() == null) {
              try {
                sets().insertOne(session, setDocument(set).append("version", 0));
              } catch (MongoWriteException ex) {
                if (ex.getError().getCode() == 11000
                    && ex.getError().getMessage().contains("uq_partition_set_key"))
                  throw new DuplicateSetKey(ex);
                throw ex;
              }
              for (PartitionEntity row : write.rows()) rows().insertOne(session, rowDocument(row));
            } else {
              long changed =
                  sets()
                      .updateOne(
                          session,
                          and(eq("_id", set.getSetId()), eq("version", write.expectedVersion())),
                          combine(
                              set("latest_generation", set.getLatestGeneration()),
                              set("latest_batch_id", set.getLatestBatchId()),
                              set("generation_started_at", Date.from(set.getGenerationStartedAt())),
                              set("updated_at", Date.from(set.getUpdatedAt())),
                              inc("version", 1)))
                      .getMatchedCount();
              if (changed == 0) return false;
              for (PartitionEntity row : write.rows()) {
                rows()
                    .updateOne(
                        session,
                        and(
                            eq("set_id", set.getSetId()),
                            eq("partition_index", row.getPartitionIndex())),
                        combine(
                            set("generation", row.getGeneration()),
                            set("child_job_id", row.getChildJobId()),
                            set("outcome", null),
                            set("last_error", null),
                            set("checkpoint_json", row.getCheckpoint())));
              }
            }
            jobs.create(session, write.parent());
            batches.saveBatch(session, write.batch());
            return true;
          },
          TRANSACTION_OPTIONS);
    } catch (DuplicateSetKey ex) {
      return false;
    }
  }

  private static final class DuplicateSetKey extends RuntimeException {
    DuplicateSetKey(Throwable cause) {
      super(cause);
    }
  }

  @Override
  public List<PartitionEntity> findPartitions(UUID setId, int afterIndex, int limit) {
    return page(and(eq("set_id", setId), gt("partition_index", afterIndex)), limit);
  }

  @Override
  public List<PartitionEntity> findGenerationPartitions(
      UUID setId, int generation, int afterIndex, int limit) {
    return page(
        and(eq("set_id", setId), eq("generation", generation), gt("partition_index", afterIndex)),
        limit);
  }

  private List<PartitionEntity> page(Bson filter, int limit) {
    if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
    List<PartitionEntity> result = new ArrayList<>();
    for (Document doc : rows().find(filter).sort(ascending("partition_index")).limit(limit))
      result.add(mapRow(doc));
    return result;
  }

  @Override
  public int sealPartitions(UUID setId, int generation, List<PartitionEntity> outcomes) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            Document current = sets().find(session, eq("_id", setId)).first();
            if (current == null || current.getInteger("sealed_generation", 0) >= generation) {
              return 0;
            }
            int changed = 0;
            for (PartitionEntity row : outcomes) {
              changed +=
                  (int)
                      rows()
                          .updateOne(
                              session,
                              and(
                                  eq("set_id", setId),
                                  eq("partition_index", row.getPartitionIndex()),
                                  eq("generation", generation),
                                  eq("child_job_id", row.getChildJobId())),
                              combine(
                                  set(
                                      "outcome",
                                      row.getOutcome() == null ? null : row.getOutcome().name()),
                                  set("last_error", row.getLastError()),
                                  set("checkpoint_json", row.getCheckpoint())))
                          .getMatchedCount();
            }
            return changed;
          },
          TRANSACTION_OPTIONS);
    }
  }

  @Override
  public boolean markSealed(UUID setId, int generation) {
    return sets()
            .updateOne(
                and(eq("_id", setId), lt("sealed_generation", generation)),
                combine(
                    set("sealed_generation", generation),
                    set("updated_at", Date.from(Instant.now()))))
            .getModifiedCount()
        > 0;
  }

  @Override
  public List<PartitionSetEntity> findUnsealedSets(UUID afterSetId, int limit) {
    if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
    List<PartitionSetEntity> result = new ArrayList<>();
    for (Document doc :
        sets()
            .find(
                afterSetId == null
                    ? unsealed()
                    : and(unsealed(), new Document("_id", new Document("$gt", afterSetId))))
            .sort(ascending("_id"))
            .limit(limit)) result.add(mapSet(doc));
    return result;
  }

  private static Bson unsealed() {
    return new Document(
        "$expr", new Document("$lt", List.of("$sealed_generation", "$latest_generation")));
  }

  @Override
  public Optional<PartitionEntity> findPartitionByChild(UUID childJobId) {
    Document row = rows().find(eq("child_job_id", childJobId)).first();
    return row == null ? Optional.empty() : Optional.of(mapRow(row));
  }

  @Override
  public long countBatchChildren(UUID batchId) {
    return ctx.jobs()
        .withReadPreference(ReadPreference.primary())
        .countDocuments(and(eq("depends_on", batchId), eq("job_type", "BATCH_CHILD")));
  }

  @Override
  public long countUnfinishedBatchChildren(UUID batchId) {
    return ctx.jobs()
        .withReadPreference(ReadPreference.primary())
        .countDocuments(
            and(
                eq("depends_on", batchId),
                eq("job_type", "BATCH_CHILD"),
                nin("status", MongoStoreContext.TERMINAL_STATUSES)));
  }

  @Override
  public boolean deletePartitionSet(UUID setId) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            Document deleted =
                sets()
                    .findOneAndDelete(
                        session,
                        and(
                            eq("_id", setId),
                            new Document(
                                "$expr",
                                new Document(
                                    "$eq", List.of("$sealed_generation", "$latest_generation")))));
            if (deleted == null) return false;
            rows().deleteMany(session, eq("set_id", setId));
            return true;
          },
          TRANSACTION_OPTIONS);
    }
  }

  /** Collects every unsealed generation, including older generations if repair is pending. */
  static Bson unprotectedJobs(MongoStoreContext ctx) {
    List<UUID> batches = new ArrayList<>();
    // Protection decisions must see the latest committed generations and their batches.
    MongoCollection<Document> sets =
        ctx.database()
            .getCollection("scheduler_partition_set")
            .withReadPreference(ReadPreference.primary());
    for (Document set : sets.find(unsealed())) {
      for (Document batch :
          ctx.batches()
              .withReadPreference(ReadPreference.primary())
              .find(
                  and(
                      eq("partition_set_id", set.get("_id")),
                      gt("generation", set.getInteger("sealed_generation"))))) {
        batches.add(batch.get("_id", UUID.class));
      }
    }
    return and(nin("_id", batches), or(ne("job_type", "BATCH_CHILD"), nin("depends_on", batches)));
  }

  private Document setDocument(PartitionSetEntity value) {
    Document doc = new Document();
    doc.append("_id", value.getSetId());
    doc.append("set_key", value.getSetKey());
    doc.append("name", value.getName());
    doc.append("partition_count", value.getPartitionCount());
    doc.append("latest_generation", value.getLatestGeneration());
    doc.append("latest_batch_id", value.getLatestBatchId());
    doc.append("sealed_generation", value.getSealedGeneration());
    doc.append("generation_started_at", Date.from(value.getGenerationStartedAt()));
    doc.append("owner_principal", value.getOwnerPrincipal());
    doc.append("version", value.getVersion());
    doc.append("created_at", Date.from(value.getCreatedAt()));
    doc.append("updated_at", Date.from(value.getUpdatedAt()));
    return doc;
  }

  private PartitionSetEntity mapSet(Document doc) {
    PartitionSetEntity value = new PartitionSetEntity();
    value.setSetId(doc.get("_id", UUID.class));
    value.setSetKey(doc.getString("set_key"));
    value.setName(doc.getString("name"));
    value.setPartitionCount(doc.getInteger("partition_count", 0));
    value.setLatestGeneration(doc.getInteger("latest_generation", 0));
    value.setLatestBatchId(doc.get("latest_batch_id", UUID.class));
    value.setSealedGeneration(doc.getInteger("sealed_generation", 0));
    value.setGenerationStartedAt(doc.getDate("generation_started_at").toInstant());
    value.setOwnerPrincipal(doc.getString("owner_principal"));
    value.setVersion(doc.getInteger("version", 0));
    value.setCreatedAt(doc.getDate("created_at").toInstant());
    value.setUpdatedAt(doc.getDate("updated_at").toInstant());
    return value;
  }

  private Document rowDocument(PartitionEntity value) {
    Document doc = new Document();
    doc.append("set_id", value.getSetId());
    doc.append("partition_index", value.getPartitionIndex());
    doc.append("range_from", value.getRangeFrom());
    doc.append("range_to", value.getRangeTo());
    doc.append("generation", value.getGeneration());
    doc.append("child_job_id", value.getChildJobId());
    doc.append("outcome", value.getOutcome() == null ? null : value.getOutcome().name());
    doc.append("last_error", value.getLastError());
    doc.append("checkpoint_json", value.getCheckpoint());
    return doc;
  }

  private PartitionEntity mapRow(Document doc) {
    PartitionEntity value = new PartitionEntity();
    value.setSetId(doc.get("set_id", UUID.class));
    value.setPartitionIndex(doc.getInteger("partition_index", 0));
    value.setRangeFrom(((Number) doc.get("range_from")).longValue());
    value.setRangeTo(((Number) doc.get("range_to")).longValue());
    value.setGeneration(doc.getInteger("generation", 0));
    value.setChildJobId(doc.get("child_job_id", UUID.class));
    value.setOutcome(
        doc.getString("outcome") == null
            ? null
            : PartitionOutcome.valueOf(doc.getString("outcome")));
    value.setLastError(doc.getString("last_error"));
    value.setCheckpoint(doc.getString("checkpoint_json"));
    return value;
  }
}
