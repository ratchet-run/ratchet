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
package run.ratchet.compatibility;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.compatibility.IsolatedJdbcRuntime.StoreDialect;

class MixedVersionPostgresqlCompatibilityTest {

  @SuppressWarnings("resource")
  private static final PostgreSQLContainer POSTGRESQL =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_test")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("stringtype", "unspecified")
          .withInitScript("ddl/postgresql-schema.sql")
          .withReuse(true);

  @BeforeAll
  static void startDatabase() {
    POSTGRESQL.start();
  }

  @AfterAll
  static void stopDatabase() {
    POSTGRESQL.stop();
  }

  @Test
  void released011AndCurrentSnapshotProcessEachOthersCommonJobs() throws Exception {
    Path reactorRoot = Path.of(System.getProperty("ratchet.compat.reactorRoot"));
    Path publishedRuntime = Path.of(System.getProperty("ratchet.compat.oldRuntime"));

    try (IsolatedJdbcRuntime released =
            IsolatedJdbcRuntime.published011(
                StoreDialect.POSTGRESQL, publishedRuntime, POSTGRESQL);
        IsolatedJdbcRuntime current =
            IsolatedJdbcRuntime.currentSnapshot(StoreDialect.POSTGRESQL, reactorRoot, POSTGRESQL)) {
      assertTrue(released.codeSource().endsWith("ratchet-store-postgresql-0.1.1.jar"));
      assertTrue(current.codeSource().endsWith("stores/ratchet-store-postgresql/target/classes/"));
      assertNotSame(released.jobEntityClass(), current.jobEntityClass());
      assertNotSame(
          released.jobEntityClass().getClassLoader(), current.jobEntityClass().getClassLoader());

      UUID releasedJob = UUID.randomUUID();
      released.createPendingJob(releasedJob, "created-by-0.1.1", "payload-from-0.1.1");
      current.consumeJob(
          releasedJob,
          "created-by-0.1.1",
          "payload-from-0.1.1",
          "current-node",
          "result-from-current");
      released.assertSucceeded(releasedJob, "payload-from-0.1.1", "result-from-current");

      UUID currentJob = UUID.randomUUID();
      current.createPendingJob(currentJob, "created-by-current", "payload-from-current");
      released.consumeJob(
          currentJob,
          "created-by-current",
          "payload-from-current",
          "0.1.1-node",
          "result-from-0.1.1");
      current.assertSucceeded(currentJob, "payload-from-current", "result-from-0.1.1");
    }
  }
}
