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

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.RuntimeProcess;

class MongoEncryptionRotationRuntimeIT {
  static final String OLD = Base64.getEncoder().encodeToString(new byte[32]);
  static final String NEW =
      Base64.getEncoder()
          .encodeToString("12345678901234567890123456789012".getBytes(StandardCharsets.US_ASCII));

  @Test
  void newWriteKeyRetainsReadAccessToPreviouslyQueuedPayloads() throws Exception {
    try (var container = run.ratchet.consumer.mongodb.MongoDatabase.create()) {
      container.start();
      String uri = container.getReplicaSetUrl("runtime_keys");
      try (var client = MongoClients.create(uri)) {
        var db = client.getDatabase("runtime_keys");
        String original = queue(uri, db, "rotation-old");
        try (var app =
            new RuntimeProcess(
                    MongoRuntimeApplication.class,
                    keys(uri, "old:" + OLD + ",new:" + NEW, "new"),
                    "mongo-rotated")
                .ready()) {
          MongoProcessesRuntimeIT.succeeded(db, "rotation-old");
          assertThat(
                  db.getCollection("runtime_attempt")
                      .find(eq("business_id", "rotation-old"))
                      .first()
                      .getString("job_id"))
              .isEqualTo(original);
          app.post("/submit?id=rotation-new");
          MongoProcessesRuntimeIT.succeeded(db, "rotation-new");
          var fresh = MongoProcessesRuntimeIT.job(db, "rotation-new");
          assertThat(fresh.getString("encryption_key_id")).isEqualTo("new");
          assertThat(fresh.get("payload").toString())
              .contains("rcph:e:")
              .doesNotContain("rotation-new");
          assertThat(
                  db.getCollection("runtime_effect")
                      .find(eq("_id", "rotation-old"))
                      .first()
                      .getInteger("executions"))
              .isEqualTo(1);
        }
      }
    }
  }

  @Test
  void missingKeyPreventsInvocationAndRestoringItAllowsReplayOfOriginalJob() throws Exception {
    try (var container = run.ratchet.consumer.mongodb.MongoDatabase.create()) {
      container.start();
      String uri = container.getReplicaSetUrl("runtime_missing_key");
      try (var client = MongoClients.create(uri)) {
        var db = client.getDatabase("runtime_missing_key");
        String original = queue(uri, db, "missing-key");
        try (var app =
            new RuntimeProcess(
                    MongoRuntimeApplication.class,
                    keys(uri, "new:" + NEW, "new"),
                    "mongo-missing-key")
                .ready()) {
          await()
              .atMost(Duration.ofSeconds(40))
              .untilAsserted(
                  () ->
                      assertThat(MongoProcessesRuntimeIT.job(db, "missing-key").getString("status"))
                          .isEqualTo("FAILED"));
          assertThat(
                  db.getCollection("runtime_attempt")
                      .countDocuments(eq("business_id", "missing-key")))
              .isZero();
          assertThat(
                  db.getCollection("runtime_effect")
                      .find(eq("_id", "missing-key"))
                      .first()
                      .getInteger("executions"))
              .isZero();
          assertThat(Files.readString(app.log)).contains("No key installed for id: old");
        }
        try (var app =
            new RuntimeProcess(
                    MongoRuntimeApplication.class,
                    keys(uri, "old:" + OLD + ",new:" + NEW, "new"),
                    "mongo-restored-key")
                .ready()) {
          assertThat(app.post("/retry?id=" + original)).isEqualTo("true");
          MongoProcessesRuntimeIT.succeeded(db, "missing-key");
          assertThat(
                  db.getCollection("runtime_attempt")
                      .find(eq("business_id", "missing-key"))
                      .first()
                      .getString("job_id"))
              .isEqualTo(original);
          assertThat(
                  db.getCollection("runtime_effect")
                      .find(eq("_id", "missing-key"))
                      .first()
                      .getInteger("executions"))
              .isEqualTo(1);
        }
      }
    }
  }

  private static String queue(String uri, MongoDatabase db, String businessKey) throws Exception {
    try (var app =
        new RuntimeProcess(
                MongoRuntimeApplication.class,
                keys(uri, "old:" + OLD, "old"),
                "mongo-original-" + businessKey)
            .ready()) {
      app.post("/drain?enabled=true");
      String original = app.post("/submit?id=" + businessKey);
      var stored = MongoProcessesRuntimeIT.job(db, businessKey);
      assertThat(stored.getString("encryption_key_id")).isEqualTo("old");
      assertThat(stored.get("payload").toString()).contains("rcph:e:").doesNotContain(businessKey);
      assertThat(db.getCollection("runtime_attempt").countDocuments(eq("business_id", businessKey)))
          .isZero();
      return original;
    }
  }

  private static Map<String, Object> keys(String uri, String keys, String current) {
    var properties = MongoProcessesRuntimeIT.properties(uri, "mongo-key-worker");
    properties.put("ratchet.encryption.enabled", true);
    properties.put("ratchet.encryption.keys", keys);
    properties.put("ratchet.encryption.current-key", current);
    return properties;
  }
}
