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
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.JobHandle;
import run.ratchet.consumer.mongodb.MongoDatabase;

class MongoConsumerTest {

  @Test
  void bootManagedMongoDatabaseRunsRatchetJobsInTheSelectedDatabase() {
    try (MongoDBContainer database = MongoDatabase.create()) {
      database.start();
      try (var context = application(database)) {
        MongoConsumerService service = context.getBean(MongoConsumerService.class);
        MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class);
        MongoDatabaseFactory databaseFactory = context.getBean(MongoDatabaseFactory.class);

        assertThat(mongoTemplate.getMongoDatabaseFactory()).isSameAs(databaseFactory);
        assertThat(context.getBeansOfType(MongoClient.class)).hasSize(1);
        assertThat(mongoTemplate.getDb().getName()).isEqualTo("ratchet_consumer");
        assertThat(databaseFactory.getMongoDatabase().getName()).isEqualTo("ratchet_consumer");

        String id = "job-" + System.nanoTime();
        JobHandle handle = service.submit(id);
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  assertThat(mongoTemplate.findById(id, ConsumerRecord.class).state())
                      .isEqualTo("executed");
                  assertThat(mongoTemplate.getDb().getCollection("scheduler_job").countDocuments())
                      .isGreaterThanOrEqualTo(1);
                });
        assertThat(handle.id()).isNotNull();
      }
    }
  }

  @Test
  void schemaAutoMigrateFalseValidatesExistingCollectionsWithoutCreatingThem() {
    try (MongoDBContainer database = MongoDatabase.create()) {
      database.start();
      try (var ignored = application(database)) {
        // Establish the externally managed schema before starting validation-only mode.
      }
      try (var context = application(database, "ratchet.schema.auto-migrate=false")) {
        MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class);
        assertThat(mongoTemplate.getDb().listCollectionNames().into(new ArrayList<>()))
            .contains("scheduler_job");
        assertThat(
                mongoTemplate
                    .getDb()
                    .getCollection("scheduler_job")
                    .listIndexes()
                    .into(new ArrayList<>()))
            .isNotEmpty();
      }
    }
  }

  @Test
  void schemaAutoMigrateFalseRejectsMissingCollectionsWithoutMutatingTheDatabase() {
    try (MongoDBContainer database = MongoDatabase.create()) {
      database.start();
      assertThatThrownBy(() -> application(database, "ratchet.schema.auto-migrate=false"))
          .hasStackTraceContaining("missing collection scheduler_job");
      try (MongoClient client =
          MongoClients.create(database.getReplicaSetUrl("ratchet_consumer"))) {
        assertThat(
                client
                    .getDatabase("ratchet_consumer")
                    .listCollectionNames()
                    .into(new ArrayList<>()))
            .isEmpty();
      }
    }
  }

  private static ConfigurableApplicationContext application(
      MongoDBContainer database, String... additionalProperties) {
    Map<String, Object> properties =
        new LinkedHashMap<>(
            MongoConsumerProperties.connection(database.getReplicaSetUrl("ratchet_consumer")));
    for (String property : additionalProperties) {
      String[] keyValue = property.split("=", 2);
      properties.put(keyValue[0], keyValue[1]);
    }
    return new SpringApplicationBuilder(MongoConsumerApplication.class)
        .properties(properties)
        .run();
  }
}
