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
package run.ratchet.store.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.AbstractSchemaMigratorContract;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin SQL Server Testcontainer (no init script). Verifies
 * that the migrator brings the schema up from empty, is idempotent on re-run, and converges under
 * {@code sp_getapplock} when two threads race.
 */
class SqlserverSchemaMigratorIT extends AbstractSchemaMigratorContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MSSQLServerContainer CONTAINER = MssqlContainers.create();

  @BeforeAll
  static void start() {
    CONTAINER.start();
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  @Override
  protected DataSource dataSource() {
    return new JdbcDriverDataSource(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected SchemaMigrationDialect dialect() {
    return new SqlserverSchemaMigrationDialect();
  }

  @Override
  protected Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  // Drop every Ratchet table child-first so foreign keys never block the drop. SQL Server has no
  // "DROP SCHEMA ... CASCADE", so the virgin state is restored table by table.
  private static final List<String> TABLES_CHILD_FIRST =
      List.of(
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_job_tag",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_workflow_condition",
          "scheduler_dlq_alerts",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive",
          "scheduler_job",
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
          "scheduler_resource_limit",
          "scheduler_lock",
          "scheduler_node",
          "ratchet_schema_version");

  @Override
  protected void resetDatabase() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      for (String table : TABLES_CHILD_FIRST) {
        s.execute("DROP TABLE IF EXISTS dbo." + table);
      }
    }
  }
}
