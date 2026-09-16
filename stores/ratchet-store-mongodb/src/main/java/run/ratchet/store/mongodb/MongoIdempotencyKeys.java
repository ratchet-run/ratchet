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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.entity.JobEntity;

/** Permanent reservations independent of job and archive retention. */
final class MongoIdempotencyKeys {
  static final String COLLECTION = "scheduler_idempotency_key";

  private MongoIdempotencyKeys() {}

  static Optional<UUID> find(MongoDatabase database, String key) {
    Document doc = database.getCollection(COLLECTION).find(eq("_id", key)).first();
    return doc == null ? Optional.empty() : Optional.of(doc.get("original_job_id", UUID.class));
  }

  static void reserve(MongoDatabase database, ClientSession session, List<JobEntity> jobs) {
    List<WriteModel<Document>> writes = new ArrayList<>(jobs.size());
    for (JobEntity job : jobs) {
      writes.add(
          new UpdateOneModel<>(
              and(eq("_id", job.getIdempotencyKey()), eq("original_job_id", job.getId())),
              new Document("$setOnInsert", new Document("reserved_at", new Date())),
              new UpdateOptions().upsert(true)));
    }
    try {
      database.getCollection(COLLECTION).bulkWrite(session, writes);
    } catch (MongoBulkWriteException e) {
      if (e.getWriteErrors().stream().anyMatch(error -> error.getCode() == 11000)) {
        int index = e.getWriteErrors().get(0).getIndex();
        throw new DuplicateIdempotencyKeyException(jobs.get(index).getIdempotencyKey(), e);
      }
      throw e;
    }
  }
}
