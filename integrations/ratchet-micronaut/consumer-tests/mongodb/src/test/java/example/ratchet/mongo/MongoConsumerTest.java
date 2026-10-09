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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.micronaut.context.ApplicationContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.consumer.mongodb.MongoDatabase;

class MongoConsumerTest {
  @Test
  void micronautManagedClientRunsRatchetInTheSelectedDatabase() {
    try (var database = MongoDatabase.create()) {
      database.start();
      try (var context = application(database)) {
        assertThat(context.getBeansOfType(MongoClient.class)).hasSize(1);
        var db = context.getBean(MongoClient.class).getDatabase("ratchet_consumer");
        assertThat(db.getName()).isEqualTo("ratchet_consumer");
        String id = "job-" + System.nanoTime();
        var handle = context.getBean(MongoConsumerService.class).submit(id);
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  assertThat(
                          db.getCollection("consumer_record")
                              .find(new Document("_id", id))
                              .first()
                              .getString("state"))
                      .isEqualTo("executed");
                  assertThat(db.getCollection("scheduler_job").countDocuments())
                      .isGreaterThanOrEqualTo(1);
                });
        assertThat(handle.id()).isNotNull();
      }
    }
  }

  @Test
  void validationOnlyAcceptsExistingCollectionsAndIndexes() {
    try (var database = MongoDatabase.create()) {
      database.start();
      try (var ignored = application(database)) {}
      try (var context = application(database, "ratchet.schema.auto-migrate=false")) {
        var db = context.getBean(MongoClient.class).getDatabase("ratchet_consumer");
        assertThat(db.listCollectionNames().into(new ArrayList<>())).contains("scheduler_job");
        assertThat(db.getCollection("scheduler_job").listIndexes().into(new ArrayList<>()))
            .isNotEmpty();
      }
    }
  }

  @Test
  void validationOnlyRejectsMissingCollectionsWithoutCreatingThem() {
    try (var database = MongoDatabase.create()) {
      database.start();
      assertThatThrownBy(() -> application(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("missing collection scheduler_job");
      try (var client = MongoClients.create(database.getReplicaSetUrl("ratchet_consumer"))) {
        assertThat(
                client
                    .getDatabase("ratchet_consumer")
                    .listCollectionNames()
                    .into(new ArrayList<>()))
            .isEmpty();
      }
    }
  }

  private static ApplicationContext application(MongoDBContainer database, String... overrides) {
    var properties =
        new HashMap<>(
            MongoConsumerProperties.connection(database.getReplicaSetUrl("ratchet_consumer")));
    for (String override : overrides) {
      String[] pair = override.split("=", 2);
      properties.put(pair[0], pair[1]);
    }
    var context = ApplicationContext.builder().properties(properties).build();
    try {
      return context.start();
    } catch (RuntimeException failure) {
      try {
        context.close();
      } catch (RuntimeException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }
}
