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
package example.ratchet.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.bson.Document;

@Singleton
public class MongoConsumerJobs {
  private final MongoCollection<Document> records;

  public MongoConsumerJobs(
      MongoClient client, @Value("${ratchet.mongodb.database}") String database) {
    records = client.getDatabase(database).getCollection("consumer_record");
  }

  public void complete(String id) {
    Document record = records.find(new Document("_id", id)).first();
    if (record == null)
      throw new IllegalStateException(
          "Application document was not persisted before execution: " + id);
    records.updateOne(
        new Document("_id", id), new Document("$set", new Document("state", "executed")));
  }
}
