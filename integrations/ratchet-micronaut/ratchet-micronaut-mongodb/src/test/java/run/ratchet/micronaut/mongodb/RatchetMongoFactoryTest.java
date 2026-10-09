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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Database selection does not require a running Mongo server. */
class RatchetMongoFactoryTest {
  @Test
  void explicitDatabaseWinsOverUri() {
    assertEquals(
        "selected",
        database(Map.of("ratchet.mongodb.database", "selected", "mongodb.uri", "invalid")));
  }

  @Test
  void fallsBackToUriDatabase() {
    assertEquals("jobs", database(Map.of("mongodb.uri", "mongodb://localhost:27017/jobs")));
    assertEquals(
        "jobs",
        database(
            Map.of(
                "ratchet.mongodb.database", " ",
                "mongodb.uri", "mongodb://localhost:27017/jobs")));
  }

  @Test
  void missingDatabaseExplainsBothConfigurationOptions() {
    for (Map<String, Object> properties :
        List.<Map<String, Object>>of(
            Map.of(), Map.of("mongodb.uri", "mongodb://localhost:27017"))) {
      var failure = assertThrows(IllegalStateException.class, () -> database(properties));
      assertTrue(failure.getMessage().contains("ratchet.mongodb.database"));
      assertTrue(failure.getMessage().contains("mongodb.uri"));
    }
  }

  private static String database(Map<String, Object> values) {
    Map<String, Object> properties = new HashMap<>(values);
    properties.put("ratchet.enabled", false);
    try (ApplicationContext context = ApplicationContext.builder().properties(properties).build()) {
      var environment = context.getEnvironment();
      environment.start();
      try {
        return RatchetMongoFactory.databaseName(environment);
      } finally {
        environment.stop();
      }
    }
  }
}
