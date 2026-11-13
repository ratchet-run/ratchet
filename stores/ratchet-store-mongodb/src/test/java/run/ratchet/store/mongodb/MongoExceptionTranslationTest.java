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

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;
import static run.ratchet.store.entity.JobExecutionEntity.SHUTDOWN_RELEASE_ERROR_MESSAGE;
import static run.ratchet.store.mongodb.MongoFieldNames.HEARTBEAT_TS;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_NODE;
import static run.ratchet.store.mongodb.MongoJobCrudOperations.ORPHAN_EXECUTION_CLOSE_BATCH;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.UpdateResult;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class MongoExceptionTranslationTest {

  @Test
  void createReservesBusinessKeyBeforeIdempotencyAndJobWrites() {
    List<String> calls = new ArrayList<>();
    MongoCollection<Document> collection =
        mongoCollection(
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "deleteOne" -> calls.add("release");
                case "insertOne" ->
                    calls.add(((Document) args[1]).containsKey("owner_table") ? "reserve" : "job");
                case "bulkWrite" -> calls.add("idempotency");
                default -> {}
              }
              return defaultValue(method);
            });
    JobEntity job = testJob();
    job.setBusinessKey("order-1");

    new MongoJobCrudOperations(contextWithJobs(collection)).create(job);

    assertEquals(List.of("release", "reserve", "idempotency", "job"), calls);
  }

  @Test
  void createPreservesReservationConflictBeforeOtherWrites() {
    BusinessKeyReservationConflictException conflict =
        new BusinessKeyReservationConflictException("order-1");
    MongoCollection<Document> collection =
        mongoCollection(
            (proxy, method, args) -> {
              if ("insertOne".equals(method.getName())) {
                if (((Document) args[1]).containsKey("owner_table")) {
                  throw conflict;
                }
                throw new AssertionError("The job must not be written before reservation");
              }
              if ("bulkWrite".equals(method.getName())) {
                throw new AssertionError("Idempotency must not be written before reservation");
              }
              return defaultValue(method);
            });
    JobEntity job = testJob();
    job.setBusinessKey("order-1");
    MongoJobCrudOperations crud = new MongoJobCrudOperations(contextWithJobs(collection));

    assertSame(
        conflict,
        assertThrows(BusinessKeyReservationConflictException.class, () -> crud.create(job)));
  }

  @Test
  void transactionOptionsRequirePrimaryReads() {
    assertEquals(
        ReadPreference.primary(), MongoStoreContext.TRANSACTION_OPTIONS.getReadPreference());
  }

  @Test
  void currentClaimTranslatesTransientReadFailure() {
    MongoSocketException failure = transientFailure();
    AtomicReference<Object> readPreference = new AtomicReference<>();
    AtomicReference<Object> readConcern = new AtomicReference<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("withReadPreference".equals(method.getName())) {
                readPreference.set(args[0]);
                return proxy;
              }
              if ("withReadConcern".equals(method.getName())) {
                readConcern.set(args[0]);
                return proxy;
              }
              if ("find".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    var crud = new MongoJobCrudOperations(contextWithJobs(jobs));
    var thrown =
        assertThrows(
            RatchetTransientStoreException.class, () -> crud.currentClaim(UUID.randomUUID()));
    assertSame(failure, thrown.getCause());
    assertEquals(ReadPreference.primary(), readPreference.get());
    assertEquals(ReadConcern.MAJORITY, readConcern.get());
  }

  @Test
  void callbackAndDependantLookupsReadThePrimary() {
    IllegalStateException primaryRead = new IllegalStateException("primary read");
    List<Object> readPreferences = new ArrayList<>();
    MongoCollection<Document> primary =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                throw primaryRead;
              }
              return defaultValue(method);
            });
    MongoCollection<Document> collection =
        mongoCollection(
            (proxy, method, args) -> {
              if ("withReadPreference".equals(method.getName())) {
                readPreferences.add(args[0]);
                return primary;
              }
              if ("find".equals(method.getName())) {
                throw new AssertionError("read used the client's read preference");
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobs(collection);
    MongoJobCrudOperations crud = new MongoJobCrudOperations(ctx);
    MongoAuxiliaryOperations auxiliary = new MongoAuxiliaryOperations(ctx);

    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class, () -> crud.findByIds(List.of(UUID.randomUUID()))));
    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class, () -> crud.findDependants(UUID.randomUUID(), 10, 0)));
    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class,
            () -> auxiliary.findConditionsByParentJobId(UUID.randomUUID())));
    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class, () -> MongoPartitionOperations.unprotectedJobs(ctx)));
    assertEquals(
        List.of(
            ReadPreference.primary(),
            ReadPreference.primary(),
            ReadPreference.primary(),
            ReadPreference.primary()),
        readPreferences);
  }

  @Test
  void partitionReadsUseThePrimary() {
    IllegalStateException primaryRead = new IllegalStateException("primary read");
    List<Object> readPreferences = new ArrayList<>();
    MongoCollection<Document> primary =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName()) || "countDocuments".equals(method.getName())) {
                throw primaryRead;
              }
              return defaultValue(method);
            });
    MongoCollection<Document> collection =
        mongoCollection(
            (proxy, method, args) -> {
              if ("withReadPreference".equals(method.getName())) {
                readPreferences.add(args[0]);
                return primary;
              }
              if ("find".equals(method.getName()) || "countDocuments".equals(method.getName())) {
                throw new AssertionError("read used the client's read preference");
              }
              return defaultValue(method);
            });
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("getCollection".equals(method.getName())) {
                return collection;
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = new MongoStoreContext(mongoClient(null), database);
    MongoPartitionOperations partitions = new MongoPartitionOperations(ctx, null, null);
    UUID id = UUID.randomUUID();

    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.findPartitionSet("set")));
    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.findPartitionSetById(id)));
    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.findPartitions(id, -1, 10)));
    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class, () -> partitions.findGenerationPartitions(id, 1, -1, 10)));
    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.findUnsealedSets(null, 10)));
    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.findPartitionByChild(id)));
    assertSame(
        primaryRead,
        assertThrows(IllegalStateException.class, () -> partitions.countBatchChildren(id)));
    assertSame(
        primaryRead,
        assertThrows(
            IllegalStateException.class, () -> partitions.countUnfinishedBatchChildren(id)));
    assertEquals(8, readPreferences.size());
    assertTrue(readPreferences.stream().allMatch(ReadPreference.primary()::equals));
  }

  @Test
  void partitionProtectionReadsSetsAndBatchesFromPrimary() {
    List<Object> preferences = new ArrayList<>();
    MongoCollection<Document> primarySets =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                return findIterable(
                    cursor(
                        List.of(
                            new Document("_id", UUID.randomUUID()).append("sealed_generation", 0)),
                        new AtomicBoolean()));
              }
              return defaultValue(method);
            });
    MongoCollection<Document> primaryBatches =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                return findIterable(cursor(List.of(), new AtomicBoolean()));
              }
              return defaultValue(method);
            });
    MongoCollection<Document> sets =
        mongoCollection(
            (proxy, method, args) -> {
              if ("withReadPreference".equals(method.getName())) {
                preferences.add(args[0]);
                return primarySets;
              }
              throw new AssertionError("Set protection read did not select primary");
            });
    MongoCollection<Document> batches =
        mongoCollection(
            (proxy, method, args) -> {
              if ("withReadPreference".equals(method.getName())) {
                preferences.add(args[0]);
                return primaryBatches;
              }
              throw new AssertionError("Batch protection read did not select primary");
            });
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("getCollection".equals(method.getName())) {
                return "scheduler_partition_set".equals(args[0]) ? sets : batches;
              }
              return defaultValue(method);
            });
    MongoPartitionOperations.unprotectedJobs(new MongoStoreContext(mongoClient(null), database));
    assertEquals(List.of(ReadPreference.primary(), ReadPreference.primary()), preferences);
  }

  @Test
  void closeStaleExecutionsTranslatesTransientFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> executions =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoJobCrudOperations crud =
        new MongoJobCrudOperations(contextWithCollections(null, null, executions, null));
    assertEquals(0, crud.closeStaleExecutions(Instant.now(), 0));
    var thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> crud.closeStaleExecutions(Instant.now(), 1));
    assertSame(failure, thrown.getCause());
  }

  @Test
  void claimNextBatchTranslatesTransientCandidateFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("aggregate".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void claimCandidateCursorClosesWhenIterationFails() {
    AtomicBoolean closed = new AtomicBoolean();
    MongoCursor<Document> cursor = cursor(List.of(new Document(ID, "not-a-uuid")), closed);
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("aggregate".equals(method.getName())) {
                return aggregateIterable(cursor);
              }
              return defaultValue(method);
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    assertThrows(
        RuntimeException.class, () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertTrue(closed.get());
  }

  @Test
  void claimReadBackCursorClosesWhenMapperFails() {
    UUID id = UUID.randomUUID();
    AtomicBoolean readBackClosed = new AtomicBoolean();
    MongoCursor<Document> candidateCursor =
        cursor(List.of(new Document(ID, id)), new AtomicBoolean());
    MongoCursor<Document> readBackCursor = cursor(List.of(new Document(ID, id)), readBackClosed);
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "aggregate" -> aggregateIterable(candidateCursor);
                case "withWriteConcern" -> proxy;
                case "getWriteConcern" -> WriteConcern.ACKNOWLEDGED;
                case "bulkWrite" -> BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of());
                case "find" -> findIterable(readBackCursor);
                default -> defaultValue(method);
              };
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    assertThrows(
        RuntimeException.class, () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertTrue(readBackClosed.get());
  }

  @Test
  void batchClaimPreservesConfiguredTimeoutAndJournal() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();

    assertTrue(fixture.claim().isEmpty());

    assertEquals(List.of(fixture.configured.withW("majority")), fixture.writeConcerns);
    assertEquals(1, fixture.readBacks);
  }

  @Test
  void tryPickUpJobPreservesConfiguredTimeoutAndJournal() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    assertTrue(fixture.pickUp());

    assertEquals(List.of(fixture.configured.withW("majority")), fixture.writeConcerns);
    assertEquals(fixture.configured.withW("majority"), fixture.pickupConcern);
  }

  @Test
  void batchWriteConcernErrorReleasesOnlyThisClaimAndPropagates() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    fixture.bulkFailure = bulkClaimFailure(true);

    IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::claim);

    assertSame(fixture.bulkFailure, thrown.getCause());
    assertEquals(0, fixture.readBacks);
    assertEquals(1, fixture.releases);
    assertEquals(
        List.of(fixture.configured.withW("majority"), fixture.configured.withW("majority")),
        fixture.writeConcerns);
    assertEquals(fixture.configured.withW("majority"), fixture.releaseConcern);
    BsonDocument claimUpdate = bson(fixture.claimOp.getUpdate());
    BsonDocument expectedFilter =
        bson(
            new Document(
                "$and",
                List.of(
                    new Document("_id", new Document("$in", List.of(fixture.id))),
                    new Document("picked_by", "node-1"),
                    new Document(
                        "picked_at", claimUpdate.getDocument("$set").getDateTime("picked_at")),
                    new Document("status", "RUNNING"))));
    assertEquals(expectedFilter, fixture.releaseFilter);
    BsonDocument releasedFields = fixture.releaseUpdate.getDocument("$set");
    assertEquals(new BsonString("PENDING"), releasedFields.get("status"));
    for (String field : List.of("picked_by", "picked_at", "execution_start_time")) {
      assertTrue(releasedFields.get(field).isNull());
    }
    assertTrue(releasedFields.get("updated_at").isDateTime());
    assertEquals(
        new BsonDocument("version", new BsonInt32(1)), fixture.releaseUpdate.getDocument("$inc"));
    assertFalse(fixture.releaseUpdate.toJson().contains("claim_seq"));
  }

  @Test
  void batchReleaseFailureIsSuppressedOnOriginalWriteConcernError() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    fixture.bulkFailure = bulkClaimFailure(true);
    fixture.releaseFailure = new MongoException("release failed");

    IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::claim);

    assertSame(fixture.bulkFailure, thrown.getCause());
    assertEquals(List.of(fixture.releaseFailure), List.of(fixture.bulkFailure.getSuppressed()));
    assertEquals(1, fixture.releases);
    assertEquals(0, fixture.readBacks);
  }

  @Test
  void tryPickUpJobWriteConcernErrorReleasesOnlyThisClaimAndPropagates() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    fixture.pickupFailure = pickupFailure();

    IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::pickUp);

    assertSame(fixture.pickupFailure, thrown.getCause());
    assertEquals(0, fixture.readBacks);
    assertEquals(1, fixture.releases);
    assertEquals(
        List.of(fixture.configured.withW("majority"), fixture.configured.withW("majority")),
        fixture.writeConcerns);
    assertEquals(fixture.configured.withW("majority"), fixture.pickupConcern);
    assertEquals(fixture.configured.withW("majority"), fixture.releaseConcern);
    BsonDocument claimUpdate = fixture.pickupUpdate;
    BsonDocument expectedFilter =
        bson(
            new Document(
                "$and",
                List.of(
                    new Document("_id", new Document("$in", List.of(fixture.id))),
                    new Document("picked_by", "node-1"),
                    new Document(
                        "picked_at", claimUpdate.getDocument("$set").getDateTime("picked_at")),
                    new Document("status", "RUNNING"))));
    assertEquals(expectedFilter, fixture.releaseFilter);
    BsonDocument releasedFields = fixture.releaseUpdate.getDocument("$set");
    assertEquals(new BsonString("PENDING"), releasedFields.get("status"));
    for (String field : List.of("picked_by", "picked_at", "execution_start_time")) {
      assertTrue(releasedFields.get(field).isNull());
    }
    assertTrue(releasedFields.get("updated_at").isDateTime());
    assertEquals(
        new BsonDocument("version", new BsonInt32(1)), fixture.releaseUpdate.getDocument("$inc"));
    assertFalse(fixture.releaseUpdate.toJson().contains("claim_seq"));
  }

  @Test
  void tryPickUpJobReleaseFailureIsSuppressedOnOriginalWriteConcernError() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    fixture.pickupFailure = pickupFailure();
    fixture.releaseFailure = new MongoException("release failed");

    IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::pickUp);

    assertSame(fixture.pickupFailure, thrown.getCause());
    assertEquals(List.of(fixture.releaseFailure), List.of(fixture.pickupFailure.getSuppressed()));
    assertEquals(1, fixture.releases);
    assertEquals(0, fixture.readBacks);
  }

  @Test
  void batchPerOperationWriteErrorStillReadsBack() {
    ClaimWriteFixture fixture = new ClaimWriteFixture();
    fixture.bulkFailure = bulkClaimFailure(false);

    assertTrue(fixture.claim().isEmpty());

    assertEquals(1, fixture.readBacks);
    assertEquals(0, fixture.releases);
  }

  private static MongoWriteConcernException pickupFailure() {
    return new MongoWriteConcernException(
        new WriteConcernError(64, "WriteConcernFailed", "majority timeout", new BsonDocument()),
        WriteConcernResult.acknowledged(1, false, null),
        new ServerAddress(),
        new HashSet<>());
  }

  private static MongoBulkWriteException bulkClaimFailure(boolean writeConcernError) {
    return new MongoBulkWriteException(
        BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of()),
        writeConcernError
            ? List.of()
            : List.of(new BulkWriteError(11000, "per-op failure", new BsonDocument(), 0)),
        writeConcernError
            ? new WriteConcernError(
                64, "WriteConcernFailed", "majority timeout", new BsonDocument())
            : null,
        new ServerAddress(),
        new HashSet<>());
  }

  private static BsonDocument bson(Bson value) {
    return value.toBsonDocument(
        Document.class,
        fromRegistries(
            fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
            MongoClientSettings.getDefaultCodecRegistry()));
  }

  private static final class ClaimWriteFixture {
    final UUID id = UUID.randomUUID();
    final WriteConcern configured =
        WriteConcern.W2.withWTimeout(1234, TimeUnit.MILLISECONDS).withJournal(true);
    final List<WriteConcern> writeConcerns = new ArrayList<>();
    WriteConcern activeConcern;
    WriteConcern pickupConcern;
    WriteConcern releaseConcern;
    MongoBulkWriteException bulkFailure;
    MongoWriteConcernException pickupFailure;
    RuntimeException releaseFailure;
    UpdateOneModel<?> claimOp;
    BsonDocument pickupUpdate;
    BsonDocument releaseFilter;
    BsonDocument releaseUpdate;
    int readBacks;
    int releases;
    final MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "getWriteConcern" -> configured;
                case "withWriteConcern" -> {
                  activeConcern = (WriteConcern) args[0];
                  writeConcerns.add(activeConcern);
                  yield proxy;
                }
                case "aggregate" ->
                    aggregateIterable(cursor(List.of(new Document(ID, id)), new AtomicBoolean()));
                case "bulkWrite" -> {
                  assertEquals(configured.withW("majority"), activeConcern);
                  claimOp = (UpdateOneModel<?>) ((List<?>) args[0]).get(0);
                  if (bulkFailure != null) {
                    throw bulkFailure;
                  }
                  yield BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of());
                }
                case "updateOne" -> {
                  pickupConcern = activeConcern;
                  pickupUpdate = bson((Bson) args[1]);
                  if (pickupFailure != null) {
                    throw pickupFailure;
                  }
                  yield UpdateResult.acknowledged(1, 1L, null);
                }
                case "updateMany" -> {
                  releases++;
                  releaseConcern = activeConcern;
                  releaseFilter = bson((Bson) args[0]);
                  releaseUpdate = bson((Bson) args[1]);
                  if (releaseFailure != null) {
                    throw releaseFailure;
                  }
                  yield UpdateResult.acknowledged(1, 1L, null);
                }
                case "find" -> {
                  readBacks++;
                  yield findIterable(cursor(List.of(), new AtomicBoolean()));
                }
                default -> defaultValue(method);
              };
            });

    boolean pickUp() {
      MongoStoreContext ctx = contextWithJobs(jobs);
      return new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx))
          .tryPickUpJob(id, "node-1");
    }

    List<JobEntity> claim() {
      return new MongoJobClaimOperations(contextWithJobs(jobs))
          .claimNextBatch(1, "node-1", NodeTagFilter.NONE);
    }
  }

  @Test
  void singleManualRetryRetriesParentConflictAndObservesResume() {
    assertManualRetryFencesParent(false);
  }

  @Test
  void bulkManualRetryRetriesParentConflictAndObservesResume() {
    assertManualRetryFencesParent(true);
  }

  private void assertManualRetryFencesParent(boolean bulk) {
    UUID parentId = UUID.randomUUID();
    UUID childId = UUID.randomUUID();
    Document child =
        new Document(ID, childId)
            .append("job_type", "BATCH_CHILD")
            .append("depends_on", parentId)
            .append("status", "FAILED")
            .append("priority", JobPriority.NORMAL.persistedCode())
            .append("backoff_policy", "NONE");
    Document pausedParent = new Document(ID, parentId).append("status", "PAUSED");
    Document resumedParent = new Document(ID, parentId).append("status", "PENDING");
    Document batch = new Document(ID, parentId).append("completion_processed", false);
    AtomicInteger transactions = new AtomicInteger();
    AtomicInteger fences = new AtomicInteger();
    AtomicInteger pausedChildren = new AtomicInteger();
    AtomicReference<ClientSession> activeSession = new AtomicReference<>();
    MongoException conflict = new MongoException(112, "parent changed after snapshot");
    conflict.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if (method.getName().equals("withTransaction")) {
                activeSession.set((ClientSession) proxy);
                TransactionBody<?> body = (TransactionBody<?>) args[0];
                transactions.incrementAndGet();
                try {
                  return body.execute();
                } catch (MongoException e) {
                  assertSame(conflict, e);
                  assertTrue(e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
                  transactions.incrementAndGet();
                  return body.execute();
                }
              }
              return defaultValue(method);
            });
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if (args != null && args.length > 0 && args[0] instanceof ClientSession) {
                assertSame(activeSession.get(), args[0]);
              }
              return switch (method.getName()) {
                case "find" -> {
                  String filter = bson((Bson) args[1]).toJson();
                  Document result =
                      filter.contains("completion_processed")
                          ? batch
                          : filter.contains("PAUSED") ? pausedParent : child;
                  yield retryDocuments(List.of(result));
                }
                case "findOneAndUpdate" -> {
                  String filter = bson((Bson) args[1]).toJson();
                  assertTrue(filter.contains("PENDING") && filter.contains("PAUSED"));
                  assertEquals(
                      new BsonDocument("$inc", new BsonDocument("version", new BsonInt32(1))),
                      bson((Bson) args[2]));
                  assertEquals(
                      ReturnDocument.AFTER,
                      ((FindOneAndUpdateOptions) args[3]).getReturnDocument());
                  if (fences.incrementAndGet() == 1) {
                    throw conflict;
                  }
                  yield resumedParent;
                }
                case "updateOne" -> {
                  if (args[2] instanceof Bson update && bson(update).toJson().contains("PAUSED")) {
                    pausedChildren.incrementAndGet();
                  }
                  yield UpdateResult.acknowledged(1, 1L, null);
                }
                case "updateMany" -> UpdateResult.acknowledged(1, 1L, null);
                default -> defaultValue(method);
              };
            });
    MongoStoreContext ctx = contextWithJobsAndSession(jobs, session);
    var lifecycle = new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx));

    if (bulk) {
      assertEquals(1, lifecycle.resetFailedToPending(JobFilter.builder().build(), 10));
    } else {
      assertTrue(lifecycle.resetFailedToPending(childId));
    }
    assertEquals(2, transactions.get(), "parent conflict must retry the entire transaction");
    assertTrue(fences.get() >= 2);
    assertEquals(0, pausedChildren.get(), "retry must use the resumed parent status");
  }

  @Test
  void repairSweepsFenceExpectedParentBeforeReadingChildren() {
    for (boolean pause : List.of(true, false)) {
      AtomicInteger parentWrites = new AtomicInteger();
      MongoCollection<Document> jobs =
          mongoCollection(
              (proxy, method, args) -> {
                if (method.getName().equals("updateOne")) {
                  String filter = bson((Bson) args[1]).toJson();
                  assertTrue(filter.contains(pause ? "PAUSED" : "PENDING"));
                  assertEquals(
                      new BsonDocument("$inc", new BsonDocument("version", new BsonInt32(1))),
                      bson((Bson) args[2]));
                  parentWrites.incrementAndGet();
                  return UpdateResult.acknowledged(0, 0L, null);
                }
                if (method.getName().equals("find")) {
                  throw new AssertionError("A stale sweep must not select children");
                }
                return defaultValue(method);
              });
      var operations = new MongoBatchOperations(contextWithJobs(jobs));
      assertEquals(
          0,
          pause
              ? operations.pauseBatchChildren(UUID.randomUUID())
              : operations.resumeBatchChildren(UUID.randomUUID()));
      assertEquals(1, parentWrites.get());
    }
  }

  private static FindIterable<Document> retryDocuments(List<Document> documents) {
    return proxy(
        FindIterable.class,
        (proxy, method, args) ->
            switch (method.getName()) {
              case "sort", "limit", "skip", "projection" -> proxy;
              case "first" -> documents.isEmpty() ? null : documents.get(0);
              case "iterator", "cursor" -> cursor(documents, new AtomicBoolean());
              default -> defaultValue(method);
            });
  }

  @Test
  void lifecycleMutationTranslatesTransientMongoFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateOne".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobs(jobs);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(
            ctx, new MongoBatchOperations(ctx), new MongoJobQueryOperations(ctx));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> lifecycle.updateJobStatus(UUID.randomUUID(), JobStatus.FAILED, "failed"));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void compareAndSwapCancelCommitsWithConfiguredMajorityWriteConcern() {
    assertStatusMutationTransaction(JobStatus.CANCELED, false);
  }

  @Test
  void cancelJobCommitsWithConfiguredMajorityWriteConcern() {
    assertStatusMutationTransaction(JobStatus.CANCELED, true);
  }

  @Test
  void compareAndSwapPauseUsesPrimaryTransactionOptions() {
    assertStatusMutationTransaction(JobStatus.PAUSED, false);
  }

  private static void assertStatusMutationTransaction(JobStatus target, boolean directCancel) {
    WriteConcern configured =
        WriteConcern.W1.withWTimeout(1234, TimeUnit.MILLISECONDS).withJournal(true);
    AtomicInteger transactions = new AtomicInteger();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "getWriteConcern" -> configured;
                case "updateOne" -> UpdateResult.acknowledged(1, 1L, null);
                case "find" -> findIterable(cursor(List.of(), new AtomicBoolean()));
                default -> defaultValue(method);
              };
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                transactions.incrementAndGet();
                assertEquals(2, args.length);
                TransactionOptions options = (TransactionOptions) args[1];
                assertEquals(ReadPreference.primary(), options.getReadPreference());
                if (target == JobStatus.CANCELED) {
                  WriteConcern concern = options.getWriteConcern();
                  assertEquals("majority", concern.getWString());
                  assertEquals(
                      configured.getWTimeout(TimeUnit.MILLISECONDS),
                      concern.getWTimeout(TimeUnit.MILLISECONDS));
                  assertEquals(configured.getJournal(), concern.getJournal());
                } else {
                  assertSame(MongoStoreContext.TRANSACTION_OPTIONS, options);
                }
                return ((TransactionBody<?>) args[0]).execute();
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobsAndSession(jobs, session);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx));
    UUID id = UUID.randomUUID();

    assertTrue(
        directCancel
            ? lifecycle.cancelJob(id)
            : lifecycle.compareAndSwapStatus(id, JobStatus.PENDING, target, null));
    assertEquals(1, transactions.get());
  }

  @Test
  void transactionalLifecycleBodyKeepsMongoExceptionVisibleToSession() {
    MongoSocketException failure = transientFailure();
    AtomicReference<RuntimeException> seenBySession = new AtomicReference<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateOne".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                try {
                  return ((TransactionBody<?>) args[0]).execute();
                } catch (RuntimeException e) {
                  seenBySession.set(e);
                  throw e;
                }
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobsAndSession(jobs, session);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(
            ctx, new MongoBatchOperations(ctx), new MongoJobQueryOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            lifecycle.markJobSucceededAndUpdateBatch(
                UUID.randomUUID(),
                "{}",
                "json",
                Instant.now(),
                Instant.now(),
                1L,
                1L,
                UUID.randomUUID()));

    assertSame(failure, seenBySession.get());
  }

  @Test
  void deliverSignalByKeyTranslatesTransientTransactionFailure() {
    MongoSocketException failure = transientFailure();
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoSignalOperations signals =
        new MongoSignalOperations(contextWithJobsAndSession(null, session));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () ->
                signals.deliverSignalByKey(
                    "approval",
                    "{}",
                    "json",
                    "APPROVED",
                    null,
                    "tester",
                    Instant.now(),
                    "delivery-1"));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void tryAcquirePermitTranslatesTransientTransactionFailure() {
    MongoSocketException failure = transientFailure();
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoAuxiliaryOperations auxiliary =
        new MongoAuxiliaryOperations(contextWithJobsAndSession(null, session));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> auxiliary.tryAcquirePermit("api", UUID.randomUUID(), "node-1"));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void tagOperationsTranslateTransientMongoFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateOne".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoTagOperations tags = new MongoTagOperations(contextWithJobs(jobs));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> tags.insertTags(UUID.randomUUID(), List.of("urgent")));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void translateStoreExceptionWrapsNonTransientMongoFailures() {
    MongoException failure = new MongoException("plain command failure");
    MongoStoreContext ctx = contextWithJobs(null);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> {
              throw ctx.translateTransientStoreException("read jobs", failure);
            });

    assertSame(failure, thrown.getCause());
    assertTrue(thrown.getMessage().contains("read jobs"));
  }

  @Test
  void bulkInsertTranslatesDuplicateBusinessKeyFailure() {
    RuntimeException failure =
        new RuntimeException(
            "bulk",
            new MongoWriteException(
                new WriteError(
                    11000,
                    "E11000 duplicate key error collection: ratchet.scheduler_job index:"
                        + " idx_job_active_business_key dup key: { business_key: \"order-1\" }",
                    new BsonDocument()),
                new ServerAddress()));
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("insertMany".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoJobCrudOperations crud = new MongoJobCrudOperations(contextWithJobs(jobs));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class, () -> crud.bulkInsert(List.of(testJob())));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void resetOrphanJobsUsesTransactionForNodeSnapshotAndMutation() {
    AtomicBoolean usedTransaction = new AtomicBoolean();
    MongoCollection<Document> nodes =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                return findIterable(
                    cursor(
                        List.of(
                            new Document(ID, "node-1")
                                .append(HEARTBEAT_TS, DocumentMapper.toDate(Instant.now()))),
                        new AtomicBoolean()));
              }
              return defaultValue(method);
            });
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                return findIterable(cursor(List.of(), new AtomicBoolean()));
              }
              if ("updateMany".equals(method.getName())) {
                return UpdateResult.acknowledged(0, 0L, null);
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                usedTransaction.set(true);
                return ((TransactionBody<?>) args[0]).execute();
              }
              return defaultValue(method);
            });
    MongoCollection<Document> executions =
        mongoCollection(
            (proxy, method, args) -> {
              throw new AssertionError("No execution update is needed without orphan ids");
            });
    MongoJobCrudOperations crud =
        new MongoJobCrudOperations(contextWithCollections(jobs, nodes, executions, session));

    assertEquals(0, crud.resetOrphanJobs(Duration.ofMinutes(5), 3, 0).reset());
    assertTrue(usedTransaction.get());
  }

  @Test
  void shutdownClosesExecutionsInTheSessionBeforeResettingJobs() {
    UUID jobId = UUID.randomUUID();
    var registry =
        fromRegistries(
            fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
            MongoClientSettings.getDefaultCodecRegistry());
    AtomicBoolean inTransaction = new AtomicBoolean();
    List<String> calls = new ArrayList<>();
    AtomicReference<ClientSession> activeSession = new AtomicReference<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("find");
                return findIterable(cursor(List.of(new Document(ID, jobId)), new AtomicBoolean()));
              }
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("reset");
                return UpdateResult.acknowledged(1, 1L, null);
              }
              return defaultValue(method);
            });
    MongoCollection<Document> executions =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("close");
                String filter = ((Bson) args[1]).toBsonDocument(Document.class, registry).toJson();
                String update = ((Bson) args[2]).toBsonDocument(Document.class, registry).toJson();
                assertTrue(filter.contains("job_id"));
                assertTrue(filter.contains("RUNNING"));
                assertTrue(update.contains("FAILED"));
                assertTrue(update.contains("ended_at"));
                assertTrue(update.contains(SHUTDOWN_RELEASE_ERROR_MESSAGE));
                return UpdateResult.acknowledged(7, 7L, null);
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                inTransaction.set(true);
                try {
                  return ((TransactionBody<?>) args[0]).execute();
                } finally {
                  inTransaction.set(false);
                }
              }
              return defaultValue(method);
            });
    activeSession.set(session);
    MongoStoreContext ctx = contextWithCollections(jobs, jobs, executions, session);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx));

    assertEquals(1, lifecycle.resetRunningJobs("dead"));
    assertEquals(List.of("find", "close", "reset"), calls);
  }

  @Test
  void startupRecoveryClosesExecutionsInTheSessionBeforeResettingJobs() {
    UUID jobId = UUID.randomUUID();
    var registry =
        fromRegistries(
            fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
            MongoClientSettings.getDefaultCodecRegistry());
    AtomicBoolean inTransaction = new AtomicBoolean();
    List<String> calls = new ArrayList<>();
    AtomicReference<ClientSession> activeSession = new AtomicReference<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                assertSame(activeSession.get(), args[0]);
                calls.add("find");
                return findIterable(cursor(List.of(new Document(ID, jobId)), new AtomicBoolean()));
              }
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("reset");
                return UpdateResult.acknowledged(1, 1L, null);
              }
              return defaultValue(method);
            });
    MongoCollection<Document> executions =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("close");
                String filter = ((Bson) args[1]).toBsonDocument(Document.class, registry).toJson();
                String update = ((Bson) args[2]).toBsonDocument(Document.class, registry).toJson();
                assertTrue(filter.contains("job_id"));
                assertTrue(filter.contains("RUNNING"));
                assertTrue(update.contains("FAILED"));
                assertTrue(update.contains("ended_at"));
                assertTrue(update.contains(ORPHAN_RECOVERY_ERROR_MESSAGE));
                return UpdateResult.acknowledged(7, 7L, null);
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                inTransaction.set(true);
                try {
                  return ((TransactionBody<?>) args[0]).execute();
                } finally {
                  inTransaction.set(false);
                }
              }
              return defaultValue(method);
            });
    activeSession.set(session);
    MongoJobCrudOperations crud =
        new MongoJobCrudOperations(contextWithCollections(jobs, jobs, executions, session));

    assertEquals(1, crud.resetOrphanJobsForNode("dead", 3, 0).reset());
    assertEquals(List.of("find", "close", "reset", "reset"), calls);
  }

  @Test
  void startupRecoveryClosesExecutionsInBatches() {
    List<UUID> jobIds = new ArrayList<>();
    List<Document> documents = new ArrayList<>();
    for (int i = 0; i < ORPHAN_EXECUTION_CLOSE_BATCH + 1; i++) {
      UUID jobId = UUID.randomUUID();
      jobIds.add(jobId);
      documents.add(new Document(ID, jobId));
    }
    var registry =
        fromRegistries(
            fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
            MongoClientSettings.getDefaultCodecRegistry());
    AtomicBoolean inTransaction = new AtomicBoolean();
    AtomicReference<ClientSession> activeSession = new AtomicReference<>();
    List<String> calls = new ArrayList<>();
    List<Integer> batchSizes = new ArrayList<>();
    List<UUID> closedIds = new ArrayList<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("find".equals(method.getName())) {
                assertSame(activeSession.get(), args[0]);
                calls.add("find");
                return findIterable(cursor(documents, new AtomicBoolean()));
              }
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("reset");
                return UpdateResult.acknowledged(jobIds.size(), (long) jobIds.size(), null);
              }
              return defaultValue(method);
            });
    MongoCollection<Document> executions =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateMany".equals(method.getName())) {
                assertTrue(inTransaction.get());
                assertSame(activeSession.get(), args[0]);
                calls.add("close");
                BsonDocument filter = ((Bson) args[1]).toBsonDocument(Document.class, registry);
                var ids =
                    filter
                        .getArray("$and")
                        .get(0)
                        .asDocument()
                        .getDocument("job_id")
                        .getArray("$in");
                batchSizes.add(ids.size());
                for (var id : ids) {
                  closedIds.add(id.asBinary().asUuid(UuidRepresentation.STANDARD));
                }
                return UpdateResult.acknowledged(ids.size(), (long) ids.size(), null);
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                inTransaction.set(true);
                try {
                  return ((TransactionBody<?>) args[0]).execute();
                } finally {
                  inTransaction.set(false);
                }
              }
              return defaultValue(method);
            });
    activeSession.set(session);
    MongoJobCrudOperations crud =
        new MongoJobCrudOperations(contextWithCollections(jobs, jobs, executions, session));

    assertEquals(jobIds.size(), crud.resetOrphanJobsForNode("dead", 3, 0).reset());
    assertEquals(List.of(ORPHAN_EXECUTION_CLOSE_BATCH, 1), batchSizes);
    assertEquals(jobIds.size(), closedIds.size());
    assertEquals(new HashSet<>(jobIds), new HashSet<>(closedIds));
    assertEquals(List.of("find", "close", "close", "reset", "reset"), calls);
  }

  @Test
  void tryLockReadsDatabaseTimeBeforeWritingLease() {
    AtomicBoolean readDatabaseTime = new AtomicBoolean();
    MongoCollection<Document> locks =
        mongoCollection(
            (proxy, method, args) -> {
              if ("findOneAndUpdate".equals(method.getName())) {
                return new Document(OWNER_NODE, "node-1");
              }
              return defaultValue(method);
            });
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("runCommand".equals(method.getName())) {
                readDatabaseTime.set(true);
                return new Document(
                    "localTime", DocumentMapper.toDate(Instant.parse("2040-01-01T00:00:00Z")));
              }
              if ("getCollection".equals(method.getName())) {
                return locks;
              }
              return defaultValue(method);
            });
    MongoNodeLockOperations locksOps =
        new MongoNodeLockOperations(new MongoStoreContext(mongoClient(null), database));

    assertTrue(locksOps.tryLock("jobArchiver", Duration.ofMinutes(5), "node-1"));

    assertTrue(readDatabaseTime.get());
  }

  @Test
  void getDatabaseTimeFallsBackToLocalClockWhenServerStatusFails() {
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("runCommand".equals(method.getName())) {
                throw new MongoCommandException(
                    new BsonDocument("ok", new BsonInt32(0))
                        .append("code", new BsonInt32(13))
                        .append("errmsg", new BsonString("unauthorized")),
                    new ServerAddress());
              }
              return defaultValue(method);
            });
    MongoNodeLockOperations locks =
        new MongoNodeLockOperations(new MongoStoreContext(mongoClient(null), database));

    assertTrue(locks.getDatabaseTime().isAfter(Instant.now().minusSeconds(5)));
  }

  private static MongoStoreContext contextWithJobs(MongoCollection<Document> jobs) {
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                return ((TransactionBody<?>) args[0]).execute();
              }
              return defaultValue(method);
            });
    return contextWithJobsAndSession(jobs, session);
  }

  private static MongoStoreContext contextWithJobsAndSession(
      MongoCollection<Document> jobs, ClientSession session) {
    return contextWithCollections(jobs, jobs, session);
  }

  private static MongoStoreContext contextWithCollections(
      MongoCollection<Document> jobs, MongoCollection<Document> nodes, ClientSession session) {
    return contextWithCollections(jobs, nodes, jobs, session);
  }

  private static MongoStoreContext contextWithCollections(
      MongoCollection<Document> jobs,
      MongoCollection<Document> nodes,
      MongoCollection<Document> executions,
      ClientSession session) {
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("getCollection".equals(method.getName())) {
                return switch ((String) args[0]) {
                  case "scheduler_node" -> nodes;
                  case "scheduler_job_execution" -> executions;
                  default -> jobs;
                };
              }
              return defaultValue(method);
            });
    return new MongoStoreContext(mongoClient(session), database);
  }

  private static MongoSocketException transientFailure() {
    return new MongoSocketException("network failure", new ServerAddress());
  }

  private static JobEntity testJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    return job;
  }

  private static AggregateIterable<Document> aggregateIterable(MongoCursor<Document> cursor) {
    return proxy(
        AggregateIterable.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "allowDiskUse", "hintString" -> proxy;
            case "cursor", "iterator" -> cursor;
            default -> defaultValue(method);
          };
        });
  }

  private static FindIterable<Document> findIterable(MongoCursor<Document> cursor) {
    return proxy(
        FindIterable.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "sort", "limit", "projection" -> proxy;
            case "cursor", "iterator" -> cursor;
            default -> defaultValue(method);
          };
        });
  }

  private static MongoCursor<Document> cursor(List<Document> documents, AtomicBoolean closed) {
    AtomicInteger index = new AtomicInteger();
    return proxy(
        MongoCursor.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "hasNext" -> index.get() < documents.size();
            case "next" -> documents.get(index.getAndIncrement());
            case "close" -> {
              closed.set(true);
              yield null;
            }
            default -> defaultValue(method);
          };
        });
  }

  private static MongoClient mongoClient(ClientSession session) {
    return proxy(
        MongoClient.class,
        (proxy, method, args) -> {
          if ("startSession".equals(method.getName())) {
            return session;
          }
          return defaultValue(method);
        });
  }

  private static ClientSession clientSession(InvocationHandler handler) {
    return proxy(ClientSession.class, handler);
  }

  private static MongoCollection<Document> mongoCollection(InvocationHandler handler) {
    return proxy(MongoCollection.class, handler);
  }

  private static MongoDatabase mongoDatabase(InvocationHandler handler) {
    return proxy(MongoDatabase.class, handler);
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<?> type, InvocationHandler handler) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              Object objectResult = objectMethodResult(proxy, method, args);
              if (objectResult != NO_OBJECT_METHOD) {
                return objectResult;
              }
              return handler.invoke(proxy, method, args);
            });
  }

  private static final Object NO_OBJECT_METHOD = new Object();

  private static Object objectMethodResult(Object proxy, Method method, Object[] args) {
    if (method.getDeclaringClass() != Object.class) {
      return NO_OBJECT_METHOD;
    }
    return switch (method.getName()) {
      case "toString" -> proxy.getClass().getInterfaces()[0].getSimpleName() + " proxy";
      case "hashCode" -> System.identityHashCode(proxy);
      case "equals" -> proxy == args[0];
      default -> NO_OBJECT_METHOD;
    };
  }

  private static Object defaultValue(Method method) {
    Class<?> returnType = method.getReturnType();
    if (!returnType.isPrimitive()) {
      return null;
    }
    if (returnType == boolean.class) {
      return false;
    }
    if (returnType == byte.class) {
      return (byte) 0;
    }
    if (returnType == short.class) {
      return (short) 0;
    }
    if (returnType == int.class) {
      return 0;
    }
    if (returnType == long.class) {
      return 0L;
    }
    if (returnType == float.class) {
      return 0F;
    }
    if (returnType == double.class) {
      return 0D;
    }
    if (returnType == char.class) {
      return '\0';
    }
    return null;
  }
}
