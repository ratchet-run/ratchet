package run.ratchet.testsuite.app;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

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

  @Inject private MongoDatabase mongoDb;

  @Override
  public void setJobUpdatedAt(UUID jobId, Instant updatedAt) {
    mongoDb
        .getCollection("scheduler_job")
        .updateOne(eq("_id", jobId), set("updated_at", Date.from(updatedAt)));
  }
}
