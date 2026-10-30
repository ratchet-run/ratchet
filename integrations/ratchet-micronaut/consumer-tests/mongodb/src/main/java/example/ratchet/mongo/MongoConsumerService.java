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
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

@Singleton
public class MongoConsumerService {
  private final MongoCollection<Document> records;
  private final JobSchedulerService scheduler;
  private final MongoConsumerJobs jobs;

  public MongoConsumerService(
      MongoClient client,
      @Value("${ratchet.mongodb.database}") String database,
      JobSchedulerService scheduler,
      MongoConsumerJobs jobs) {
    records = client.getDatabase(database).getCollection("consumer_record");
    this.scheduler = scheduler;
    this.jobs = jobs;
  }

  public JobHandle submit(String id) {
    records.insertOne(new Document("_id", id).append("state", "submitted"));
    return scheduler.enqueue(() -> jobs.complete(id)).submit();
  }
}
