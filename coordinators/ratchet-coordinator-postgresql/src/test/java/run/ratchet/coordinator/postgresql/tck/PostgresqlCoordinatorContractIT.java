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
package run.ratchet.coordinator.postgresql.tck;

import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.coordinator.postgresql.PostgresqlCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorContract} against a real PostgreSQL container.
 *
 * <p>One container is shared across the class; each test provisions a fresh harness with a unique
 * NOTIFY channel so concurrent tests do not cross-talk.
 */
class PostgresqlCoordinatorContractIT extends AbstractClusterCoordinatorContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_coord_tck")
          .withUsername("ratchet")
          .withPassword("ratchet");

  private static DataSource dataSource;

  @BeforeAll
  static void start() {
    CONTAINER.start();
    dataSource =
        PostgresqlCoordinatorTestHarness.newDataSource(
            CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  @Override
  protected CoordinatorTestHarness harness() {
    return new PostgresqlCoordinatorTestHarness(dataSource);
  }
}
