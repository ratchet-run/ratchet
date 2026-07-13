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
import java.util.List;
import java.util.Objects;
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
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
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

  @Override
  public void deleteSchedulerLock(String name) {
    mongoDb
        .getCollection("scheduler_lock")
        .deleteOne(new Document("_id", Objects.requireNonNull(name, "name")));
  }
}
