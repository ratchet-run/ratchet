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
package example.runtime.mongo;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import example.ratchet.mongo.MongoConsumerProperties;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.RuntimeProcess;
import run.ratchet.consumer.TcpFaultProxy;

class MongoProcessesRuntimeIT {
  static Map<String, Object> properties(String uri, String node) {
    var properties = MongoConsumerProperties.connection(uri);
    properties.put("spring.main.web-application-type", "servlet");
    properties.put("server.port", 0);
    properties.put("ratchet.allowed-packages", "example.ratchet,example.runtime");
    properties.put("ratchet.node.id", node);
    properties.put("ratchet.node.heartbeat-interval-seconds", 1);
    properties.put("ratchet.node.orphan-grace-seconds", 3);
    properties.put("ratchet.node.orphan-scan-interval-minutes", 1);
    properties.put("ratchet.node.dynamic-heartbeat-enabled", false);
    properties.put("ratchet.poller.min-delay-ms", 100);
    properties.put("ratchet.poller.max-delay-ms", 500);
    return properties;
  }

  @Test
  void liveWorkersDeduplicateAndSurvivorRecoversOriginalJob() throws Exception {
    try (var container = run.ratchet.consumer.mongodb.MongoDatabase.create()) {
      container.start();
      String uri = container.getReplicaSetUrl("runtime_cluster");
      try (var client = MongoClients.create(uri);
          var first =
              new RuntimeProcess(
                      MongoRuntimeApplication.class,
                      properties(uri, "mongo-one"),
                      "mongo-cluster-one")
                  .ready();
          var second =
              new RuntimeProcess(
                      MongoRuntimeApplication.class,
                      properties(uri, "mongo-two"),
                      "mongo-cluster-two")
                  .ready()) {
        var db = client.getDatabase("runtime_cluster");
        first.post("/drain?enabled=true");
        second.post("/drain?enabled=true");
        var a = CompletableFuture.supplyAsync(() -> submit(first, "raced"));
        var b = CompletableFuture.supplyAsync(() -> submit(second, "raced"));
        String aResult = a.get(25, TimeUnit.SECONDS);
        String bResult = b.get(25, TimeUnit.SECONDS);
        assertThat(aResult != null || bResult != null).isTrue();
        String original = first.post("/submit?id=raced");
        if (aResult != null) assertThat(aResult).isEqualTo(original);
        if (bResult != null) assertThat(bResult).isEqualTo(original);
        assertThat(db.getCollection("scheduler_job").countDocuments(eq("business_key", "raced")))
            .isEqualTo(1);
        first.post("/drain?enabled=false");
        second.post("/drain?enabled=false");
        succeeded(db, "raced");
        assertThat(second.post("/submit?id=raced")).isEqualTo(original);
        assertThat(
                db.getCollection("runtime_effect")
                    .find(eq("_id", "raced"))
                    .first()
                    .getInteger("executions"))
            .isEqualTo(1);
        second.post("/drain?enabled=true");
        db.getCollection("runtime_gate")
            .insertOne(new Document("_id", "crash").append("released", false));
        String recoveryId = first.post("/submit?id=recovered&gate=crash");
        await()
            .atMost(Duration.ofSeconds(25))
            .untilAsserted(
                () ->
                    assertThat(
                            db.getCollection("runtime_attempt")
                                .find(eq("business_id", "recovered"))
                                .first())
                        .isNotNull());
        first.kill();
        db.getCollection("runtime_gate").updateOne(eq("_id", "crash"), set("released", true));
        second.post("/drain?enabled=false");
        await()
            .atMost(Duration.ofSeconds(100))
            .untilAsserted(
                () -> assertThat(job(db, "recovered").getString("status")).isEqualTo("SUCCEEDED"));
        assertThat(second.process.isAlive()).isTrue();
        assertThat(
                db.getCollection("runtime_attempt")
                    .distinct("job_id", eq("business_id", "recovered"), String.class)
                    .into(new ArrayList<>()))
            .containsExactly(recoveryId);
        assertThat(
                db.getCollection("runtime_attempt")
                    .distinct("node_id", eq("business_id", "recovered"), String.class)
                    .into(new ArrayList<>()))
            .containsExactlyInAnyOrder("mongo-one", "mongo-two");
        assertThat(
                db.getCollection("runtime_effect")
                    .find(eq("_id", "recovered"))
                    .first()
                    .getInteger("executions"))
            .isEqualTo(1);
      }
    }
  }

  @Test
  void disconnectedMongoClientRecoversWithoutRestartingApplication() throws Exception {
    try (var container = run.ratchet.consumer.mongodb.MongoDatabase.create()) {
      container.start();
      String uri = container.getReplicaSetUrl("runtime_outage");
      URI endpoint = URI.create(uri);
      try (var wire = new TcpFaultProxy(endpoint.getHost(), endpoint.getPort());
          var client = MongoClients.create(uri)) {
        String proxied =
            "mongodb://127.0.0.1:"
                + wire.port()
                + "/runtime_outage?directConnection=true&serverSelectionTimeoutMS=1000&connectTimeoutMS=1000&socketTimeoutMS=1000";
        try (var app =
            new RuntimeProcess(
                    MongoRuntimeApplication.class,
                    properties(proxied, "mongo-outage"),
                    "mongo-outage")
                .ready()) {
          var db = client.getDatabase("runtime_outage");
          app.post("/drain?enabled=true");
          String original = app.post("/submit?id=outage");
          wire.disconnect();
          app.post("/drain?enabled=false");
          await().atMost(Duration.ofSeconds(10)).until(() -> wire.rejected.get() > 0);
          await()
              .during(Duration.ofSeconds(3))
              .atMost(Duration.ofSeconds(5))
              .until(() -> app.process.isAlive());
          wire.reconnect();
          succeeded(db, "outage");
          assertThat(
                  db.getCollection("runtime_attempt")
                      .find(eq("business_id", "outage"))
                      .first()
                      .getString("job_id"))
              .isEqualTo(original);
          String following = app.post("/submit?id=following");
          succeeded(db, "following");
          assertThat(following).isNotEqualTo(original);
        } finally {
          wire.reconnect();
        }
      }
    }
  }

  static Document job(MongoDatabase db, String key) {
    return db.getCollection("scheduler_job").find(eq("business_key", key)).first();
  }

  static void succeeded(MongoDatabase db, String key) {
    await()
        .atMost(Duration.ofSeconds(45))
        .untilAsserted(() -> assertThat(job(db, key).getString("status")).isEqualTo("SUCCEEDED"));
  }

  private static String submit(RuntimeProcess process, String id) {
    try {
      return process.post("/submit?id=" + id);
    } catch (RuntimeProcess.HttpFailure conflict) {
      assertThat(conflict.status).isEqualTo(409);
      assertThat(conflict.body).isEqualTo("duplicate-idempotency");
      return null;
    } catch (Exception failure) {
      throw new RuntimeException(failure);
    }
  }
}
