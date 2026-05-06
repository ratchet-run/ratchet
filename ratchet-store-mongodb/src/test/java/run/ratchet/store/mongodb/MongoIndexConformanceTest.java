package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.util.HashSet;
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
          .withReuse(true)
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
  void schedulerJob_hasClaimExecIndex() {
    Set<String> names = indexNamesOn("scheduler_job");
    assertTrue(
        names.contains(MongoIndexHints.JOB_CLAIM_EXEC),
        "scheduler_job must have '"
            + MongoIndexHints.JOB_CLAIM_EXEC
            + "' (used as hint in claimNextBatchOptimized)");
  }

  @Test
  void schedulerJob_hasClaimRecurringIndex() {
    Set<String> names = indexNamesOn("scheduler_job");
    assertTrue(
        names.contains(MongoIndexHints.JOB_CLAIM_RECURRING),
        "scheduler_job must have '"
            + MongoIndexHints.JOB_CLAIM_RECURRING
            + "' (used as hint in claimDueRecurring)");
  }

  @Test
  void schedulerJob_hasIdempotencyKeyUniqueIndex() {
    Set<String> names = indexNamesOn("scheduler_job");
    assertTrue(
        names.contains("idx_job_idempotency_key"), "idempotency key unique index must exist");
  }

  @Test
  void schedulerJob_hasActiveBusinessKeyPartialUniqueIndex() {
    Set<String> names = indexNamesOn("scheduler_job");
    assertTrue(
        names.contains("idx_job_active_business_key"),
        "active business key partial unique index must exist");
  }

  @Test
  void schedulerDlqAlerts_hasJobHashUniqueIndex() {
    Set<String> names = indexNamesOn("scheduler_dlq_alerts");
    assertTrue(names.contains("idx_dlq_job_hash"), "DLQ job+hash unique index must exist");
  }

  private Set<String> indexNamesOn(String collectionName) {
    Set<String> names = new HashSet<>();
    for (Document doc : database.getCollection(collectionName).listIndexes()) {
      names.add(doc.getString("name"));
    }
    return names;
  }
}
