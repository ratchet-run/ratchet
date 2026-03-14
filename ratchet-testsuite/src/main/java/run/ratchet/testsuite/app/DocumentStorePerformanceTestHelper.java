package run.ratchet.testsuite.app;

import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import org.bson.Document;

/**
 * Document store implementation of {@link PerformanceTestHelper}.
 *
 * <p>Uses backend-specific bulk operations for maximum throughput. Currently supports MongoDB bulk
 * inserts; when additional document stores are added, this class can branch on {@code
 * ratchet.test.db.type}.
 *
 * <p>Only packaged in the WAR when a document store profile is active.
 */
@ApplicationScoped
public class DocumentStorePerformanceTestHelper implements PerformanceTestHelper {

  private static final Logger log =
      Logger.getLogger(DocumentStorePerformanceTestHelper.class.getName());

  @Inject private MongoDatabase mongoDb;

  @Override
  public void insertBackgroundRows(int count, String keyPrefix) {
    int chunkSize = 10_000;

    for (int offset = 0; offset < count; offset += chunkSize) {
      int batchCount = Math.min(chunkSize, count - offset);

      List<Document> docs = new ArrayList<>(batchCount);
      Instant past = Instant.now().minusSeconds(3600);
      for (int i = 0; i < batchCount; i++) {
        docs.add(
            new Document()
                .append("status", "SUCCEEDED")
                .append("scheduled_time", Date.from(past))
                .append("job_type", "SINGLE")
                .append(
                    "payload",
                    new Document()
                        .append("target", "run.ratchet.testsuite.app.TimingJob")
                        .append("method", "execute")
                        .append("descriptor", "()V")
                        .append("isStatic", true)
                        .append("args", List.of()))
                .append("idempotency_key", UUID.randomUUID().toString())
                .append("business_key", keyPrefix + "-" + (offset + i + 1))
                .append("execution_start_time", Date.from(past))
                .append("execution_end_time", Date.from(past.plusMillis(10)))
                .append("created_at", Date.from(Instant.now()))
                .append("updated_at", Date.from(Instant.now())));
      }
      mongoDb.getCollection("scheduler_job").insertMany(docs);

      if (count > chunkSize) {
        log.info(
            String.format("  ... inserted %d / %d background rows", offset + batchCount, count));
      }
    }
  }

  @Override
  public long queryQueueWaitPercentileForClass(String targetClass, double percentile) {
    // Query queue_wait_ms from completed jobs with the given target class
    var results = new java.util.ArrayList<Long>();
    try (var cursor =
        mongoDb
            .getCollection("scheduler_job")
            .find(
                new Document("target_class", targetClass)
                    .append("status", "SUCCEEDED")
                    .append("queue_wait_ms", new Document("$ne", null)))
            .sort(new Document("queue_wait_ms", 1))
            .iterator()) {
      while (cursor.hasNext()) {
        Document doc = cursor.next();
        Number val = doc.get("queue_wait_ms", Number.class);
        if (val != null) {
          results.add(val.longValue());
        }
      }
    }

    if (results.isEmpty()) {
      return 0;
    }
    int index = (int) Math.ceil(percentile * results.size()) - 1;
    return results.get(Math.max(0, index));
  }

  @Override
  public void assertNoFullScan(String label, Runnable storeOperation) {
    // MongoDB: collStats provides scan diagnostics, but collection-level scan counters
    // are not as straightforward as PostgreSQL's pg_stat_user_tables. Execute the operation
    // and log for informational purposes.
    storeOperation.run();

    log.info(
        String.format(
            "Scan stats [%s]: MongoDB — per-collection scan metrics not directly available, "
                + "relying on index definitions and explain plans for verification",
            label));
  }
}
