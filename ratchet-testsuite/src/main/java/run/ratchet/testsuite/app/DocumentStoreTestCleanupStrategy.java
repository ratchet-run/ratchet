package run.ratchet.testsuite.app;

import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.logging.Logger;
import org.bson.Document;

/**
 * Document store implementation of {@link TestCleanupStrategy}.
 *
 * <p>Clears all scheduler collections using backend-specific APIs. Currently supports MongoDB; when
 * additional document stores are added (DynamoDB, etc.), this class can branch on {@code
 * ratchet.test.db.type} the same way {@link JpaTestCleanupStrategy} branches for MySQL vs
 * PostgreSQL.
 *
 * <p>Only packaged in the WAR when a document store profile is active.
 */
@ApplicationScoped
public class DocumentStoreTestCleanupStrategy implements TestCleanupStrategy {

  private static final Logger log =
      Logger.getLogger(DocumentStoreTestCleanupStrategy.class.getName());

  private static final List<String> COLLECTIONS =
      List.of(
          "scheduler_workflow_condition",
          "scheduler_dlq_alerts",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_job_tag",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive",
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_job",
          "scheduler_lock",
          "scheduler_resource_limit",
          "scheduler_node",
          "counters");

  @Inject private MongoDatabase mongoDb;

  @Override
  public void truncateAll() {
    for (String collection : COLLECTIONS) {
      try {
        mongoDb.getCollection(collection).deleteMany(new Document());
      } catch (Exception e) {
        log.fine("Truncate skipped for " + collection + ": " + e.getMessage());
      }
    }
  }
}
