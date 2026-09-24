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

import example.ratchet.mongo.MongoConsumerApplication;
import example.ratchet.mongo.MongoConsumerProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.consumer.SpringApiContracts;
import run.ratchet.tck.api.ListenerProbe;

class MongoApiContractTest extends SpringApiContracts {

  private static MongoDBContainer database;
  private static ConfigurableApplicationContext application;

  @BeforeAll
  static void start() {
    database = MongoDatabase.create();
    database.start();
    application =
        new SpringApplicationBuilder(MongoConsumerApplication.class, ProbeConfiguration.class)
            .properties(properties())
            .run();
  }

  @AfterAll
  static void stop() {
    if (application != null) {
      application.close();
    }
    if (database != null) {
      database.stop();
    }
  }

  @Override
  protected ConfigurableApplicationContext context() {
    return application;
  }

  @Override
  protected void resetDatabase() {
    MongoTemplate mongoTemplate = application.getBean(MongoTemplate.class);
    mongoTemplate
        .getDb()
        .listCollectionNames()
        .forEach(name -> mongoTemplate.getDb().getCollection(name).deleteMany(new Document()));
  }

  @Override
  protected boolean transactionalStore() {
    return false;
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class ProbeConfiguration {
    @Bean
    ListenerProbe listenerProbe() {
      return new ListenerProbe();
    }
  }

  private static Map<String, Object> properties() {
    Map<String, Object> properties =
        new LinkedHashMap<>(
            MongoConsumerProperties.connection(database.getReplicaSetUrl("ratchet_contract")));
    properties.put("ratchet.allowed-packages", "example.ratchet.mongo,run.ratchet.tck.api");
    return properties;
  }
}
