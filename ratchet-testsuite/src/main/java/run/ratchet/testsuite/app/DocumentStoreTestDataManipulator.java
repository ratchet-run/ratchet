package run.ratchet.testsuite.app;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Document store implementation of {@link TestDataManipulator}.
 *
 * <p>Uses backend-specific APIs to manipulate test data. Currently supports MongoDB; when
 * additional document stores are added, this class can branch on {@code ratchet.test.db.type}.
 *
 * <p>Only packaged in the WAR when a document store profile is active.
 */
@ApplicationScoped
public class DocumentStoreTestDataManipulator implements TestDataManipulator {

  private static final Logger log =
      Logger.getLogger(DocumentStoreTestDataManipulator.class.getName());

  @Inject private MongoDatabase mongoDb;

  @Override
  public void setJobUpdatedAt(UUID jobId, Instant updatedAt) {
    UpdateResult result =
        mongoDb
            .getCollection("scheduler_job")
            .updateOne(eq("_id", jobId), set("updated_at", Date.from(updatedAt)));
    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("MongoDB did not acknowledge updated_at update for " + jobId);
    }
    if (result.getMatchedCount() == 0) {
      throw new IllegalStateException("No scheduler_job document found for " + jobId);
    }
    if (result.getModifiedCount() == 0) {
      log.warning("scheduler_job updated_at was already " + updatedAt + " for " + jobId);
    }
  }
}
