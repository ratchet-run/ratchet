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

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;
import run.ratchet.consumer.ConsumerProcess;

/**
 * Proves a killed packaged MongoDB worker recovers and completes its original job after restart.
 */
class MongoRecoveryIT {
  private static final String DATABASE = "ratchet_recovery";
  private static final String NODE_ID = "packaged-mongodb-recovery-node";

  @Test
  void killedWorkerIsRecoveredAndItsOriginalJobSucceedsAfterRestart() throws Exception {
    try (var container = run.ratchet.consumer.mongodb.MongoDatabase.create()) {
      container.start();
      String uri = container.getReplicaSetUrl(DATABASE);
      String recordId = "recovery-" + System.nanoTime();
      try (MongoClient client = MongoClients.create(uri)) {
        MongoDatabase database = client.getDatabase(DATABASE);
        AtomicReference<Object> jobId = new AtomicReference<>();

        Path firstLog = Path.of("target", "recovery-first.log");
        Process first = start(uri, firstLog, recordId, true);
        try {
          await()
              .atMost(Duration.ofSeconds(60))
              .untilAsserted(
                  () -> {
                    assertThat(first.isAlive())
                        .withFailMessage(
                            "First consumer exited before claiming work: %s",
                            Files.readString(firstLog))
                        .isTrue();
                    assertThat(recordState(database, recordId)).isEqualTo("submitted");
                    Document job = onlyJob(database);
                    assertThat(job.getString("status")).isEqualTo("RUNNING");
                    jobId.set(requireUuidIdentity(job.get("_id")));
                    MongoPackagedConsumerIT.assertCiphertext(job, recordId);
                  });
        } finally {
          first.destroyForcibly();
          assertThat(first.waitFor(20, TimeUnit.SECONDS)).isTrue();
        }

        Path secondLog = Path.of("target", "recovery-second.log");
        Process second = start(uri, secondLog, recordId, false);
        try {
          assertThat(second.waitFor(120, TimeUnit.SECONDS))
              .withFailMessage("Recovered consumer did not exit: %s", Files.readString(secondLog))
              .isTrue();
          assertThat(second.exitValue())
              .withFailMessage("%s", Files.readString(secondLog))
              .isZero();
          assertThat(Files.readString(secondLog)).contains("RATCHET_MONGODB_CONSUMER_VERIFIED");
        } finally {
          second.destroyForcibly();
          second.waitFor(10, TimeUnit.SECONDS);
        }

        assertThat(recordState(database, recordId)).isEqualTo("executed");
        assertThat(database.getCollection("scheduler_job").countDocuments()).isEqualTo(1);
        Document recovered =
            database.getCollection("scheduler_job").find(eq("_id", jobId.get())).first();
        assertThat(recovered).isNotNull();
        assertThat(recovered.getString("status")).isEqualTo("SUCCEEDED");
        MongoPackagedConsumerIT.assertCiphertext(recovered, recordId);
      }
    }
  }

  private static Process start(String uri, Path log, String recordId, boolean block)
      throws Exception {
    List<String> arguments =
        new ArrayList<>(
            ConsumerProcess.command(
                "mongodb-consumer",
                "--consumer.verify=true",
                "--ratchet.encryption.enabled=true",
                "--ratchet.encryption.current-key=native-test",
                "--ratchet.encryption.keys=native-test:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "--consumer.verify-id=" + recordId,
                "--consumer.verify-timeout-seconds=60",
                "--ratchet.node.id=" + NODE_ID));
    if (block) {
      arguments.add("--consumer.block-job-id=" + recordId);
    } else {
      arguments.add("--consumer.verify-submit=false");
    }
    String uriProperty =
        SpringBootVersion.getVersion().startsWith("3.")
            ? "spring.data.mongodb.uri"
            : "spring.mongodb.uri";
    arguments.add("--" + uriProperty + "=" + uri);
    return new ProcessBuilder(arguments)
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
  }

  private static String recordState(MongoDatabase database, String id) {
    Document record = database.getCollection("consumer_record").find(eq("_id", id)).first();
    assertThat(record).isNotNull();
    return record.getString("state");
  }

  private static Document onlyJob(MongoDatabase database) {
    List<Document> jobs = database.getCollection("scheduler_job").find().into(new ArrayList<>());
    assertThat(jobs).hasSize(1);
    return jobs.get(0);
  }

  private static Object requireUuidIdentity(Object value) {
    if (value instanceof UUID) return value;
    if (value instanceof Binary binary) {
      assertThat(binary.getType()).isEqualTo(BsonBinarySubType.UUID_STANDARD.getValue());
      assertThat(binary.getData()).hasSize(16);
      return binary;
    }
    throw new AssertionError("Ratchet job id was not stored as a UUID: " + value);
  }
}
