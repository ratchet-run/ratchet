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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Guards the {@code ddl/mongodb-init.js} bootstrap script against drift from {@link
 * MongoCollectionInitializer}, which is the source of truth. The runtime always creates indexes
 * through the Java initializer, so the script gets no container coverage; this string check keeps
 * an operator running it by hand from ending up with a different index set.
 *
 * <p>It asserts every contract-critical index is named in the script, plus the {@code unique} flag
 * on {@code uk_rec_business_key} that stops two recurring masters from registering under the same
 * business key.
 */
class MongoInitScriptConformanceTest {

  private static final String[] REQUIRED_INDEX_NAMES = {
    MongoIndexHints.JOB_CLAIM_EXEC,
    "idx_job_idempotency_key",
    "idx_job_active_business_key",
    "idx_signal_key_status",
    "idx_signal_timeout_status",
    "idx_signal_delivery_id",
    MongoIndexHints.RECURRING_JOB_CLAIM,
    "uk_rec_business_key",
    "idx_dlq_job_hash",
    "idx_wfc_evaluation_order",
  };

  private static String readInitScript() throws IOException {
    try (InputStream in =
        MongoInitScriptConformanceTest.class.getResourceAsStream("/ddl/mongodb-init.js")) {
      assertNotNull(in, "ddl/mongodb-init.js must be on the classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  void initScript_namesEveryRequiredIndex() throws IOException {
    String script = readInitScript();
    for (String indexName : REQUIRED_INDEX_NAMES) {
      assertTrue(
          script.contains("\"" + indexName + "\""),
          "ddl/mongodb-init.js is missing required index " + indexName);
    }
  }

  @Test
  void initScript_marksRecurringBusinessKeyUnique() throws IOException {
    String script = readInitScript();
    int nameIndex = script.indexOf("\"uk_rec_business_key\"");
    assertTrue(nameIndex >= 0, "ddl/mongodb-init.js must declare uk_rec_business_key");

    int blockStart = script.lastIndexOf("createIndex", nameIndex);
    int blockEnd = script.indexOf(';', nameIndex);
    assertTrue(blockStart >= 0 && blockEnd > blockStart, "uk_rec_business_key block is malformed");

    String block = script.substring(blockStart, blockEnd);
    assertTrue(
        block.contains("unique: true"),
        "uk_rec_business_key must be unique to stop duplicate recurring-master registration");
  }
}
