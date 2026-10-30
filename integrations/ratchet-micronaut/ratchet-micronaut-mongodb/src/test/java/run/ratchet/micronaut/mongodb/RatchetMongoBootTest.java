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
package run.ratchet.micronaut.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.dockerjava.api.model.Ulimit;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.micronaut.aop.Intercepted;
import io.micronaut.context.ApplicationContext;
import io.micronaut.transaction.TransactionOperations;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.micronaut.internal.NoTransactionOperations;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.store.mongodb.MongoJobStore;
import run.ratchet.store.spi.JobStore;

/** Exercises the real scheduler and transaction interceptors in a Mongo-only application. */
@Testcontainers
class RatchetMongoBootTest {
  // Mirror the store fixtures: multi-document transactions require a replica set.
  @Container
  static final MongoDBContainer MONGO =
      new MongoDBContainer("mongo:7")
          .withCreateContainerCmdModifier(
              command ->
                  command
                      .getHostConfig()
                      .withUlimits(List.of(new Ulimit("nofile", 65536L, 65536L))))
          .withReplicaSet()
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void runsJobWithoutJpaTransactionManager(boolean explicitDatabase) throws Exception {
    String database = "ratchet_micronaut_" + UUID.randomUUID().toString().replace("-", "");
    String uri = MONGO.getConnectionString();
    int query = uri.indexOf('?');
    String address = query < 0 ? uri : uri.substring(0, query);
    String parameters = query < 0 ? "" : uri.substring(query + 1) + "&";
    // Ratchet validates STANDARD UUID codecs at startup, as it does in the Spring port.
    uri =
        address.replaceAll("/+$", "")
            + "/"
            + (explicitDatabase ? "ignored" : database)
            + "?"
            + parameters
            + "uuidRepresentation=standard";
    Map<String, Object> properties =
        new HashMap<>(
            Map.of(
                "mongodb.uri",
                uri,
                "ratchet.allowed-packages",
                "run.ratchet.micronaut.mongodb",
                "ratchet.schema.auto-migrate",
                true,
                "ratchet.poller.min-delay-ms",
                100,
                "ratchet.poller.max-delay-ms",
                500));
    if (explicitDatabase) {
      properties.put("ratchet.mongodb.database", database);
    }
    try (MongoClient cleanup = MongoClients.create(uri)) {
      try (ApplicationContext context =
          ApplicationContext.builder().environments("test").properties(properties).build()) {
        context.start();
        JobStore store = context.getBean(JobStore.class);
        assertInstanceOf(MongoJobStore.class, store);
        TransactionOperations<?> transactions = context.getBean(TransactionOperations.class);
        assertInstanceOf(NoTransactionOperations.class, transactions);
        assertFalse(transactions.findTransactionStatus().isPresent());
        JobSchedulerService scheduler = context.getBean(JobSchedulerService.class);
        // Prove this exercises the compiled core proxies, rather than unadvised plain objects.
        assertInstanceOf(Intercepted.class, scheduler);
        assertInstanceOf(Intercepted.class, context.getBean(JobStateManager.class));
        var handle = scheduler.enqueueNow(MongoTestJob::execute);
        Instant deadline = Instant.now().plusSeconds(30);
        JobStatus status;
        do {
          status = store.findById(handle.id()).orElseThrow().getStatus();
          if (status == JobStatus.SUCCEEDED || status == JobStatus.FAILED) {
            break;
          }
          Thread.sleep(100);
        } while (Instant.now().isBefore(deadline));
        assertEquals(JobStatus.SUCCEEDED, status);
        assertTrue(
            context
                .getBean(MongoClient.class)
                .getDatabase(database)
                .listCollectionNames()
                .iterator()
                .hasNext());
      } finally {
        // The context drains its workers before the disposable database is removed.
        cleanup.getDatabase(database).drop();
      }
    }
  }
}
