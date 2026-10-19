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
package run.ratchet.consumer.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import example.ratchet.mongo.MongoConsumerProperties;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.ProcessScenarios;

class MongoRecoveryIT {
  @Test
  void verifiesApplicationProcess() throws Exception {
    try (var database = MongoDatabase.create()) {
      database.start();
      var options =
          ProcessScenarios.options(
              MongoConsumerProperties.connection(database.getReplicaSetUrl("native_consumer")));
      String id = ProcessScenarios.recovery("mongodb-consumer", options);
      try (var client =
          MongoClients.create(
              (String)
                  MongoConsumerProperties.connection(database.getReplicaSetUrl("native_consumer"))
                      .get("mongodb.uri"))) {
        var jobs = client.getDatabase("native_consumer").getCollection("scheduler_job");
        var row = jobs.find(new Document("_id", UUID.fromString(id))).first();
        assertThat(row).isNotNull();
        assertThat(row.getString("status")).isEqualTo("SUCCEEDED");
      }
    }
  }
}
