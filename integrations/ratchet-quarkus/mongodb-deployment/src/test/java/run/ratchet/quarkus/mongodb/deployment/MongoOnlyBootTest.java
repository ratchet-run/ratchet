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
package run.ratchet.quarkus.mongodb.deployment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.quarkus.test.QuarkusUnitTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.store.spi.JobStore;

/** Proves the MongoDB flavor boots without Hibernate ORM or a JPA persistence unit. */
class MongoOnlyBootTest {

  private static final MongoDBContainer MONGO =
      new MongoDBContainer("mongo:7.0")
          .withReplicaSet()
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  static {
    MONGO.start();
  }

  @RegisterExtension
  static final QuarkusUnitTest unitTest =
      new QuarkusUnitTest()
          .withApplicationRoot(
              jar ->
                  jar.addAsResource(
                      new StringAsset(
                          """
                          quarkus.mongodb.connection-string=%s
                          quarkus.mongodb.database=ratchet_quarkus_mongo_boot
                          quarkus.mongodb.devservices.enabled=false
                          ratchet.circuit-breaker.enabled=false
                          """
                              .formatted(MONGO.getConnectionString())),
                      "application.properties"));

  @Inject MongoClient client;
  @Inject MongoDatabase database;
  @Inject JobStore store;

  @Test
  void bootsMongoStoreWithoutJpa() {
    assertNotNull(client);
    assertNotNull(database);
    assertNotNull(store);
    assertDoesNotThrow(() -> database.listCollectionNames().first());
  }
}
