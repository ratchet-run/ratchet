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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Updates.set;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.List;
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
  private static final List<String> TERMINAL_STATUSES = List.of("SUCCEEDED", "FAILED", "CANCELED");

  @Inject private MongoDatabase mongoDb;

  @Override
  public void setJobUpdatedAt(UUID jobId, Instant updatedAt) {
    UpdateResult result =
        mongoDb
            .getCollection("scheduler_job")
            .updateOne(eq("_id", jobId), set("updated_at", Date.from(updatedAt)));
    UpdateResult terminalResult =
        mongoDb
            .getCollection("scheduler_job")
            .updateOne(
                and(eq("_id", jobId), in("status", TERMINAL_STATUSES)),
                set("terminated_at", Date.from(updatedAt)));
    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("MongoDB did not acknowledge updated_at update for " + jobId);
    }
    if (!terminalResult.wasAcknowledged()) {
      throw new IllegalStateException(
          "MongoDB did not acknowledge terminated_at update for " + jobId);
    }
    if (result.getMatchedCount() == 0) {
      throw new IllegalStateException("No scheduler_job document found for " + jobId);
    }
    if (result.getModifiedCount() == 0) {
      log.warning("scheduler_job updated_at was already " + updatedAt + " for " + jobId);
    }
  }

  @Override
  public void setArchivedAt(UUID archiveId, Instant archivedAt) {
    UpdateResult result =
        mongoDb
            .getCollection("scheduler_job_archive")
            .updateOne(eq("_id", archiveId), set("archived_at", Date.from(archivedAt)));
    if (!result.wasAcknowledged()) {
      throw new IllegalStateException(
          "MongoDB did not acknowledge archived_at update for " + archiveId);
    }
    if (result.getMatchedCount() == 0) {
      throw new IllegalStateException("No scheduler_job_archive document found for " + archiveId);
    }
    if (result.getModifiedCount() == 0) {
      log.warning(
          "scheduler_job_archive archived_at was already " + archivedAt + " for " + archiveId);
    }
  }
}
