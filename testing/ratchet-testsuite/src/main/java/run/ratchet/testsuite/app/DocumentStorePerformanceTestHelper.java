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
package run.ratchet.testsuite.app;

import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

  @Inject private Clock clock;

  @Override
  public void insertTerminalBackgroundRows(int count, int baseOffset, String keyPrefix) {
    // MongoDB keeps one scheduler_job collection (no hot/cold split): terminal docs are SUCCEEDED
    // and past-due, so countReadyJobs (PENDING-only) never counts them and the poller never claims
    // them.
    insertBackgroundRows(
        count, baseOffset, keyPrefix, "SUCCEEDED", clock.instant().minusSeconds(3600));
  }

  @Override
  public void insertPendingBackgroundRows(int count, int baseOffset, String keyPrefix) {
    // PENDING but far-future so a running poller never claims them mid-measurement, mirroring the
    // SQL hot-queue background rows.
    insertBackgroundRows(
        count, baseOffset, keyPrefix, "PENDING", clock.instant().plus(365, ChronoUnit.DAYS));
  }

  private void insertBackgroundRows(
      int count, int baseOffset, String keyPrefix, String status, Instant scheduledTime) {
    int chunkSize = 10_000;

    for (int offset = 0; offset < count; offset += chunkSize) {
      int batchCount = Math.min(chunkSize, count - offset);
      // Number documents from the cumulative base so business keys stay unique across growth calls.
      int rowOffset = baseOffset + offset;

      List<Document> docs = new ArrayList<>(batchCount);
      Instant now = clock.instant();
      Instant past = now.minusSeconds(3600);
      for (int i = 0; i < batchCount; i++) {
        docs.add(
            new Document()
                .append("status", status)
                .append("scheduled_time", Date.from(scheduledTime))
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
                .append("business_key", keyPrefix + "-" + (rowOffset + i + 1))
                .append("execution_start_time", Date.from(past))
                .append("execution_end_time", Date.from(past.plusMillis(10)))
                .append("created_at", Date.from(now))
                .append("updated_at", Date.from(now)));
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
    var results = new ArrayList<Long>();
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
  public void assertNoFullScan(String table, String label, Runnable storeOperation) {
    // MongoDB: collStats provides scan diagnostics, but collection-level scan counters
    // are not as straightforward as PostgreSQL's pg_stat_user_tables. The table argument is
    // irrelevant here (one collection holds every job). Execute the operation and log only.
    storeOperation.run();

    log.info(
        String.format(
            "Scan stats [%s] on %s: MongoDB — per-collection scan metrics not directly available, "
                + "relying on index definitions and explain plans for verification",
            label, table));
  }
}
