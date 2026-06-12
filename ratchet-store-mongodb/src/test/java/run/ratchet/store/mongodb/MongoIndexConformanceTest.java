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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.ERROR_HASH;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IDEMPOTENCY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Verifies that {@link MongoCollectionInitializer} creates the required named indexes on {@code
 * scheduler_job}. The hint names in {@link MongoIndexHints} are used directly in {@link
 * MongoJobClaimOperations} to force the query planner onto covering indexes — if an index is
 * missing or renamed, claim queries degrade silently (or throw, depending on MongoDB version).
 */
class MongoIndexConformanceTest {

  private static final MongoDBContainer MONGO =
      new MongoDBContainer("mongo:7.0")
          .withReplicaSet()
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  static {
    MONGO.start();
  }

  private MongoClient client;
  private MongoDatabase database;

  @BeforeEach
  void setUp() {
    client = MongoClientFactory.create(MONGO.getConnectionString());
    database =
        client.getDatabase("ratchet_idx_test_" + UUID.randomUUID().toString().substring(0, 8));
    new MongoCollectionInitializer(database).initialize();
  }

  @AfterEach
  void tearDown() {
    database.drop();
    client.close();
  }

  @Test
  void everyHintNamesAnIndexThatGetsCreated() throws IllegalAccessException {
    Set<String> created = allCreatedIndexNames();
    for (Field field : MongoIndexHints.class.getDeclaredFields()) {
      if (field.getType() != String.class || !Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      field.setAccessible(true);
      String hintName = (String) field.get(null);
      assertTrue(
          created.contains(hintName),
          "MongoIndexHints." + field.getName() + " (" + hintName + ") names no created index");
    }
  }

  private Set<String> allCreatedIndexNames() {
    Set<String> names = new HashSet<>();
    for (String collection : database.listCollectionNames()) {
      for (Document idx : database.getCollection(collection).listIndexes()) {
        names.add(idx.getString("name"));
      }
    }
    return names;
  }

  @Test
  void schedulerJob_hasClaimExecIndex() {
    assertIndex(
        "scheduler_job",
        MongoIndexHints.JOB_CLAIM_EXEC,
        new Document(STATUS, 1)
            .append(JOB_TYPE, 1)
            .append(PRIORITY, -1)
            .append(SCHEDULED_TIME, 1)
            .append(ID, 1),
        false,
        null);
  }

  @Test
  void schedulerJob_hasIdempotencyKeyUniqueIndex() {
    assertIndex(
        "scheduler_job", "idx_job_idempotency_key", new Document(IDEMPOTENCY_KEY, 1), true, null);
  }

  @Test
  void schedulerJob_hasActiveBusinessKeyPartialUniqueIndex() {
    Document expectedPartialFilter =
        new Document(
                STATUS, new Document("$in", List.of("PENDING", "RUNNING", "PAUSED", "WAITING")))
            .append(BUSINESS_KEY, new Document("$type", "string"));

    assertIndex(
        "scheduler_job",
        "idx_job_active_business_key",
        new Document(BUSINESS_KEY, 1),
        true,
        expectedPartialFilter);
  }

  @Test
  void schedulerDlqAlerts_hasJobHashUniqueIndex() {
    assertIndex(
        "scheduler_dlq_alerts",
        "idx_dlq_job_hash",
        new Document(JOB_ID, 1).append(ERROR_HASH, 1),
        true,
        null);
  }

  @Test
  void initialize_continuesWhenOptionalIndexCannotBeCreated() {
    database.drop();
    database
        .getCollection("scheduler_job")
        .createIndex(Indexes.ascending("legacy_tags"), new IndexOptions().name("idx_job_tags"));

    assertDoesNotThrow(() -> new MongoCollectionInitializer(database).initialize());

    Document index = indexByName("scheduler_job", "idx_job_tags");
    assertNotNull(index);
    assertEquals(new Document("legacy_tags", 1), index.get("key", Document.class));
  }

  @Test
  void initialize_failsWhenRequiredIndexCannotBeCreated() {
    database.drop();
    database
        .getCollection("scheduler_job")
        .createIndex(
            Indexes.ascending(TAGS), new IndexOptions().name(MongoIndexHints.JOB_CLAIM_EXEC));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> new MongoCollectionInitializer(database).initialize());

    assertTrue(thrown.getMessage().contains(MongoIndexHints.JOB_CLAIM_EXEC));
  }

  private void assertIndex(
      String collectionName,
      String indexName,
      Document expectedKey,
      boolean expectedUnique,
      Document expectedPartialFilter) {
    Document index = indexByName(collectionName, indexName);
    assertNotNull(index, collectionName + " must have index " + indexName);
    assertEquals(expectedKey, index.get("key", Document.class), indexName + " key mismatch");
    assertEquals(expectedUnique, index.getBoolean("unique", false), indexName + " uniqueness");
    if (expectedPartialFilter == null) {
      assertTrue(
          !index.containsKey("partialFilterExpression"),
          indexName + " should not define a partial filter");
    } else {
      assertEquals(
          expectedPartialFilter,
          index.get("partialFilterExpression", Document.class),
          indexName + " partial filter mismatch");
    }
  }

  private Document indexByName(String collectionName, String indexName) {
    for (Document doc : database.getCollection(collectionName).listIndexes()) {
      if (indexName.equals(doc.getString("name"))) {
        return doc;
      }
    }
    return null;
  }
}
