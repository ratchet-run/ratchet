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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import example.ratchet.mongo.MongoConsumerProperties;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.HashMap;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.consumer.MicronautApiContracts;
import run.ratchet.tck.api.ListenerProbe;

class MongoApiContractTest extends MicronautApiContracts {
  private static MongoDBContainer database;
  private static ApplicationContext application;

  @BeforeAll
  static void start() {
    database = MongoDatabase.create();
    database.start();
    application = launch();
  }

  private static ApplicationContext launch() {
    var properties =
        new HashMap<>(
            MongoConsumerProperties.connection(database.getReplicaSetUrl("ratchet_contract")));
    properties.put("consumer.api-contract", true);
    properties.put("ratchet.allowed-packages", "example.ratchet.mongo,run.ratchet.tck.api");
    var context = ApplicationContext.run(properties);
    context.getBean(ListenerProbe.class);
    return context;
  }

  @AfterAll
  static void stop() {
    try {
      if (application != null) application.close();
    } finally {
      if (database != null) database.stop();
    }
  }

  protected ApplicationContext context() {
    return application;
  }

  protected boolean transactionalStore() {
    return false;
  }

  protected void restartRuntime() {
    application.close();
    try (MongoClient client = MongoClients.create(database.getReplicaSetUrl("ratchet_contract"))) {
      var db = client.getDatabase("ratchet_contract");
      db.listCollectionNames().forEach(name -> db.getCollection(name).deleteMany(new Document()));
    }
    application = launch();
  }

  @Factory
  @Requires(property = "consumer.api-contract", value = "true")
  static class ProbeConfiguration {
    // Micronaut never injects fields or runs @PostConstruct on factory products.
    @Singleton
    @Bean(preDestroy = "detach")
    ListenerProbe listenerProbe(JobSchedulerService scheduler) {
      return ListenerProbe.attachedTo(scheduler);
    }
  }
}
